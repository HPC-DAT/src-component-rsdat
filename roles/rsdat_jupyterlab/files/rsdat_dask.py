"""Dask on Slurm for the RS-DAT workspace (installed by src-component-rsdat).

RSDATSLURMCluster is dask_jobqueue.SLURMCluster for a workspace that shares no filesystem or
private network with the cluster. Where the scheduler runs depends on ``rsdat.scheduler-location``
(set by the component):

- ``cluster`` (default): the scheduler runs as a Slurm job too. Scheduler and workers talk over
  the cluster's internal network, and the scheduler job opens a reverse SSH tunnel to this
  workspace, which the notebook (and the Dask sidebar and dashboard) connect through. Only the
  cluster opens connections, so this works when the workspace can't reach the compute nodes.
  The SSH key may only open listening ports on the workspace's 127.0.0.1 (authorized_keys
  options set by the component).
- ``workspace``: the scheduler runs in this JupyterLab, as with SLURMCluster. Needs connections
  in both directions; workers reach it at ``rsdat.scheduler-contact-host``.

In both, TLS is on when the dask config requires encryption (``distributed.comm.require-encryption``,
set by the component), with the workspace's certificates (``distributed.comm.tls.*``).
dask-jobqueue itself only passes certificate *file paths* to the jobs, which assumes a shared
filesystem; here the job script writes the certificates (and the tunnel key) to the node's local
scratch when the job starts (mode 600, removed when the job ends).

    from rsdat_dask import RSDATSLURMCluster
    from distributed import Client
    cluster = RSDATSLURMCluster()
    cluster.scale(jobs=2)
    client = Client(cluster)       # or Client("tls://...") - TLS settings come from the config
"""
import asyncio
import json
import logging
import shlex
import socket
import time
from urllib.parse import urlsplit

import dask
from dask_jobqueue import SLURMCluster
from dask_jobqueue.slurm import SLURMJob
from distributed.core import rpc
from distributed.deploy.spec import ProcessInterface
from distributed.security import Security
from distributed.utils import format_dashboard_link

__all__ = ["RSDATSLURMCluster", "RSDATSLURMJob", "RSDATSchedulerJob", "RSDATRemoteScheduler",
           "worker_scheduler_address"]

logger = logging.getLogger(__name__)

# Directory on the node where the job writes the TLS files (expanded by the job's shell)
REMOTE_TLS_DIR = "$RSDAT_TLS_DIR"
_PEM_DELIMITER = "RSDAT_PEM_END"


def scheduler_on_cluster():
    return dask.config.get("rsdat.scheduler-location", default="workspace") == "cluster"


def worker_scheduler_address(scheduler_address, contact_host=None):
    """The scheduler address to give the workers: same protocol and port, other host."""
    if contact_host is None and scheduler_on_cluster():
        return scheduler_address      # workers get the scheduler's address in the cluster
    if contact_host is None:
        contact_host = dask.config.get("rsdat.scheduler-contact-host", default=None)
    if not contact_host or not scheduler_address:
        return scheduler_address
    parts = urlsplit(scheduler_address)
    if not parts.port:
        return scheduler_address
    host = f"[{contact_host}]" if ":" in contact_host else contact_host
    return f"{parts.scheme}://{host}:{parts.port}"


def _pem(value):
    """PEM text of a Security field: in-memory PEM, or the contents of a local file."""
    if "-----BEGIN" in value:
        return value
    with open(value) as f:
        return f.read()


def _private_dir_lines():
    """Job-script lines that create the job's private directory on the node's local scratch,
    removed when the job ends."""
    return [
        'RSDAT_TLS_DIR=$(mktemp -d "${TMPDIR:-/tmp}/rsdat-tls.XXXXXX")',
        "trap 'rm -rf \"$RSDAT_TLS_DIR\"' EXIT",
    ]


def _file_lines(path, text):
    """Job-script lines that write text to path (mode 600)."""
    text = text.strip("\n")
    if _PEM_DELIMITER in text:
        raise ValueError("unexpected heredoc delimiter in file data")
    return [f"(umask 077; cat > \"{path}\") <<'{_PEM_DELIMITER}'", *text.splitlines(), _PEM_DELIMITER]


def ship_worker_security(security, private_dir=True):
    """Job-script lines that write the worker's TLS files on the node, and a Security object
    pointing at those files. (security, []) without TLS files."""
    if security is None:
        return None, []
    worker = security.get_tls_config_for_role("worker")
    if not any(worker.get(k) for k in ("ca_file", "cert", "key")):
        return security, []

    lines = _private_dir_lines() if private_dir else []
    remote = {}
    for key, fname, attr in (("ca_file", "ca.pem", "tls_ca_file"),
                             ("cert", "worker.pem", "tls_worker_cert"),
                             ("key", "worker.key", "tls_worker_key")):
        value = worker.get(key)
        if not value:
            continue
        path = f"{REMOTE_TLS_DIR}/{fname}"
        lines += _file_lines(path, _pem(value))
        remote[attr] = path
    remote_security = Security(
        require_encryption=security.require_encryption,
        tls_ciphers=security.tls_ciphers,
        **remote,
    )
    return remote_security, lines


class RSDATSLURMJob(SLURMJob):
    def __init__(self, scheduler=None, name=None, security=None, job_script_prologue=None, **kwargs):
        if job_script_prologue is None:
            job_script_prologue = dask.config.get(
                f"jobqueue.{self.config_name}.job-script-prologue", default=None
            ) or []
        remote_security, tls_lines = ship_worker_security(security)
        super().__init__(
            scheduler=worker_scheduler_address(scheduler),
            name=name,
            security=remote_security,
            job_script_prologue=list(job_script_prologue) + tls_lines,
            **kwargs,
        )


def _free_local_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _read(value):
    with open(value) as f:
        return f.read()


class RSDATSchedulerJob(SLURMJob):
    """The Slurm job that runs the Dask scheduler on the cluster and the reverse SSH tunnel to
    this workspace (rsdat.scheduler-location: cluster).

    The job script: the workers' prologue (log file, worker-network address, container
    settings), the TLS files and the tunnel key, then the scheduler in the background and the
    tunnel (restarted when it drops). The job ends when the scheduler ends, or when the tunnel
    can't be opened for a while (nobody can reach the scheduler anymore)."""

    def __init__(self, tunnel_port, dashboard_port, security=None, **kwargs):
        cfg = dask.config.get("rsdat.scheduler", default={}) or {}
        tunnel = dask.config.get("rsdat.tunnel", default={}) or {}
        prologue = list(dask.config.get(f"jobqueue.{self.config_name}.job-script-prologue",
                                        default=None) or [])
        remote_security, tls_lines = ship_worker_security(security, private_dir=False)
        super().__init__(
            scheduler="tcp://scheduler-placeholder:0",
            name="dask-scheduler",
            job_name="dask-scheduler",
            cores=cfg.get("cores", 1),
            processes=1,
            memory=cfg.get("memory", "2GiB"),
            walltime=cfg.get("walltime"),
            job_extra_directives=list(cfg.get("job-extra-directives") or []),
            job_script_prologue=prologue,
            **kwargs,
        )
        python = dask.config.get(f"jobqueue.{self.config_name}.python")
        self._job_script_prologue = (list(self._job_script_prologue) + _private_dir_lines()
                                     + tls_lines + self._tunnel_files(tunnel))
        self._command_template = self._scheduler_and_tunnel(
            python, remote_security, tunnel, tunnel_port, dashboard_port, cfg)

    @staticmethod
    def _tunnel_files(tunnel):
        host = tunnel["host"]
        port = int(tunnel.get("port", 22))
        name = host if port == 22 else f"[{host}]:{port}"
        known = [f"{name} {line.split()[0]} {line.split()[1]}"
                 for line in _read(tunnel["host-key-file"]).splitlines() if line.strip()]
        return (_file_lines(f"{REMOTE_TLS_DIR}/tunnel.key", _read(tunnel["key-file"]))
                + _file_lines(f"{REMOTE_TLS_DIR}/known_hosts", "\n".join(known)))

    @staticmethod
    def _scheduler_and_tunnel(python, security, tunnel, tunnel_port, dashboard_port, cfg):
        q = shlex.quote
        tls = security is not None and bool(security.tls_worker_cert)
        sched = [python, "-m", "distributed.cli.dask_scheduler",
                 '--host "${RSDAT_WORKER_IP:-$(hostname)}"', "--port 0",
                 "--dashboard-address 127.0.0.1:0",
                 '--scheduler-file "$RSDAT_TLS_DIR/scheduler.json"']
        env = []
        if tls:
            sched += ["--protocol tls", f'--tls-ca-file "{security.tls_ca_file}"',
                      f'--tls-cert "{security.tls_worker_cert}"',
                      f'--tls-key "{security.tls_worker_key}"']
            if security.require_encryption:
                env.append("export DASK_DISTRIBUTED__COMM__REQUIRE_ENCRYPTION=True")
        idle = cfg.get("idle-timeout")
        if idle:
            sched.append(f"--idle-timeout {q(str(idle))}")
        dest = f"{tunnel.get('user', 'rsdat')}@{tunnel['host']}"
        ssh = ["ssh", "-N", "-p", str(int(tunnel.get("port", 22))),
               '-i "$RSDAT_TLS_DIR/tunnel.key"', "-o BatchMode=yes", "-o IdentitiesOnly=yes",
               '-o UserKnownHostsFile="$RSDAT_TLS_DIR/known_hosts"', "-o StrictHostKeyChecking=yes",
               "-o ExitOnForwardFailure=yes", "-o ServerAliveInterval=15",
               "-o ServerAliveCountMax=4", '"${fwd[@]}"', q(dest)]
        give_up = int(cfg.get("tunnel-give-up", 600))
        return "\n".join(env + [
            # the scheduler, in the background; its address is in the scheduler file
            " ".join(sched) + " &",
            "RSDAT_SCHED_PID=$!",
            "RSDAT_SSH_PID=",
            'trap \'kill $RSDAT_SSH_PID $RSDAT_SCHED_PID 2>/dev/null; rm -rf "$RSDAT_TLS_DIR"\' EXIT',
            "for _ in $(seq 300); do",
            '    [ -s "$RSDAT_TLS_DIR/scheduler.json" ] && break',
            '    kill -0 $RSDAT_SCHED_PID 2>/dev/null || { echo "rsdat: the scheduler stopped" >&2; exit 1; }',
            "    sleep 1",
            "done",
            'RSDAT_SCHED_ADDR=$(sed -nE \'s|.*"address": *"[a-z]+://([^"]+)".*|\\1|p\' "$RSDAT_TLS_DIR/scheduler.json" | head -1)',
            'RSDAT_DASH_PORT=$(sed -nE \'s|.*"dashboard": *([0-9]+).*|\\1|p\' "$RSDAT_TLS_DIR/scheduler.json" | head -1)',
            'test -n "$RSDAT_SCHED_ADDR" || { echo "rsdat: no scheduler address" >&2; exit 1; }',
            'echo "rsdat: scheduler at $RSDAT_SCHED_ADDR, tunnel to ' + dest + '"',
            # workspace 127.0.0.1:<tunnel port> -> scheduler; :<dashboard port> -> dashboard
            f'fwd=(-R "127.0.0.1:{int(tunnel_port)}:$RSDAT_SCHED_ADDR")',
            f'[ -n "$RSDAT_DASH_PORT" ] && fwd+=(-R "127.0.0.1:{int(dashboard_port)}:127.0.0.1:$RSDAT_DASH_PORT")',
            # keep the tunnel up while the scheduler runs; give up after a while without one
            "RSDAT_TUNNEL_OK=$(date +%s)",
            "while kill -0 $RSDAT_SCHED_PID 2>/dev/null; do",
            "    RSDAT_SSH_START=$(date +%s)",
            "    " + " ".join(ssh) + " &",
            "    RSDAT_SSH_PID=$!",
            "    wait -n $RSDAT_SCHED_PID $RSDAT_SSH_PID",
            "    kill -0 $RSDAT_SCHED_PID 2>/dev/null || break",
            "    now=$(date +%s)",
            # a tunnel that stayed up for a minute was working: count from now
            "    [ $((now - RSDAT_SSH_START)) -ge 60 ] && RSDAT_TUNNEL_OK=$now",
            f"    if [ $((now - RSDAT_TUNNEL_OK)) -ge {give_up} ]; then",
            f'        echo "rsdat: no tunnel to {dest} for {give_up}s, stopping the scheduler" >&2',
            "        kill $RSDAT_SCHED_PID; break",
            "    fi",
            '    echo "rsdat: tunnel closed, reconnecting" >&2',
            "    sleep 10",
            "done",
            "wait $RSDAT_SCHED_PID",
        ])


class RSDATRemoteScheduler(ProcessInterface):
    """The cluster's scheduler as seen from this workspace: submits RSDATSchedulerJob and waits
    until the scheduler answers through the tunnel.

    address / contact_address: the scheduler's address in the cluster (given to the workers);
    external_address: the tunnel on this workspace (used by the cluster object and clients)."""

    def __init__(self, security=None, protocol=None, **_scheduler_options):
        super().__init__()
        self.security = security
        self.job = None
        self.contact_address = None
        self.external_address = None
        self.dashboard_port = None

    async def start(self):
        cfg = dask.config.get("rsdat.scheduler", default={}) or {}
        tunnel_port, dashboard_port = _free_local_port(), _free_local_port()
        while dashboard_port == tunnel_port:
            dashboard_port = _free_local_port()
        self.job = RSDATSchedulerJob(tunnel_port, dashboard_port, security=self.security)
        await self.job.start()
        tls = self.security is not None and self.security.get_tls_config_for_role("client").get("cert")
        address = f"{'tls' if tls else 'tcp'}://127.0.0.1:{tunnel_port}"
        timeout = float(cfg.get("start-timeout", 900))
        deadline = time.monotonic() + timeout
        logger.info("Waiting for scheduler job %s (tunnel %s)", self.job.job_id, address)
        connection_args = self.security.get_connection_args("client") if self.security else {}
        while True:
            try:
                async with rpc(address, timeout=5, connection_args=connection_args) as r:
                    identity = await r.identity()
                break
            except (OSError, asyncio.TimeoutError):
                state = await self._job_state()
                if state not in ("PENDING", "CONFIGURING", "RUNNING", "UNKNOWN"):
                    raise RuntimeError(
                        f"Dask scheduler job {self.job.job_id} ended ({state}) before it could be "
                        "reached through the tunnel; see its log on the cluster "
                        "(dask-scheduler-<job id>.log in the worker log directory)")
                if time.monotonic() > deadline:
                    await self.job.close()
                    raise RuntimeError(
                        f"Dask scheduler job {self.job.job_id} not reachable after {timeout:.0f}s "
                        f"(state {state}); job cancelled")
                await asyncio.sleep(3)
        self.address = self.contact_address = identity["address"]
        self.external_address = address
        if "dashboard" in (identity.get("services") or {}):
            self.dashboard_port = dashboard_port
        logger.info("Scheduler %s reachable at %s", self.address, address)
        await super().start()

    async def _job_state(self):
        """State of the scheduler job, from squeue (ENDED when it's no longer listed)."""
        try:
            out = await self.job._call(["squeue", "--json"])
            jobs = json.loads(out).get("jobs", [])
        except Exception:                     # squeue through the REST API failed: keep waiting
            return "UNKNOWN"
        for job in jobs:
            if str(job.get("job_id")) == str(self.job.job_id):
                state = job.get("job_state")
                return (state[0] if isinstance(state, list) and state else state) or "UNKNOWN"
        return "ENDED"

    async def close(self):
        if self.job is not None:
            await self.job.close()
        await super().close()


class RSDATSLURMCluster(SLURMCluster):
    job_cls = RSDATSLURMJob

    def __init__(self, *args, security=None, **kwargs):
        if security is None and dask.config.get("distributed.comm.require-encryption", default=False):
            # the workspace's certificates from the dask config (distributed.comm.tls.*)
            security = Security()
        if scheduler_on_cluster():
            kwargs.setdefault("scheduler_cls", RSDATRemoteScheduler)
        super().__init__(*args, security=security, **kwargs)

    def _get_worker_security(self, security):
        # dask-jobqueue would dump in-memory credentials to a shared directory and pass paths;
        # RSDATSLURMJob ships the certificates in the job script instead.
        return security

    @property
    def dashboard_link(self):
        # scheduler on the cluster: the dashboard is behind the tunnel on this workspace
        port = getattr(self.scheduler, "dashboard_port", None)
        if port:
            return format_dashboard_link("127.0.0.1", port)
        return super().dashboard_link

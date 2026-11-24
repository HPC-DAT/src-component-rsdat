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
- ``workspace``: the scheduler runs in this JupyterLab, as with SLURMCluster. Workers reach it at
  ``rsdat.scheduler-contact-host``. For the other direction, ``rsdat.worker-route``:

  - ``gateway``: a small gateway job on the cluster opens a reverse dynamic SSH forward
    (``ssh -R 127.0.0.1:<port>``) to this workspace, i.e. a SOCKS proxy into the cluster. This
    process connects to addresses in ``rsdat.gateway.network`` (the workers) through it; other
    connections go directly. For when the workspace can't connect to the compute nodes.
  - ``direct``: the scheduler connects to the workers directly.

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
import atexit
import ipaddress
import json
import logging
import os
import select
import shlex
import socket
import struct
import subprocess
import threading
import time
from urllib.parse import urlsplit

import dask
from dask_jobqueue import SLURMCluster
from dask_jobqueue.slurm import SLURMJob
from distributed.comm import tcp as _tcp
from distributed.core import rpc
from distributed.deploy.spec import ProcessInterface
from distributed.security import Security
from distributed.utils import format_dashboard_link
from tornado.ioloop import PeriodicCallback
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient

__all__ = ["RSDATSLURMCluster", "RSDATSLURMJob", "RSDATSchedulerJob", "RSDATRemoteScheduler",
           "RSDATGatewayJob", "worker_scheduler_address"]

logger = logging.getLogger(__name__)

# Directory on the node where the job writes the TLS files (expanded by the job's shell)
REMOTE_TLS_DIR = "$RSDAT_TLS_DIR"
_PEM_DELIMITER = "RSDAT_PEM_END"


def scheduler_on_cluster():
    return dask.config.get("rsdat.scheduler-location", default="workspace") == "cluster"


def uses_gateway():
    """Scheduler on the workspace, workers reached through the gateway job."""
    return (not scheduler_on_cluster()
            and dask.config.get("rsdat.worker-route", default="direct") == "gateway")


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
    async def start(self):
        try:
            await super().start()
        except RuntimeError as e:
            message = _refused_hint(self, e)
            # scaling runs in the background: also log it where a notebook shows it ("distributed"
            # logs to the kernel's stderr); the Dask sidebar only shows the worker count
            logging.getLogger("distributed.deploy").error("%s", message)
            raise RuntimeError(message) from None

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


def _tunnel_files(tunnel):
    """Job-script lines that write the tunnel key and the workspace's host key (known_hosts)."""
    host = tunnel["host"]
    port = int(tunnel.get("port", 22))
    name = host if port == 22 else f"[{host}]:{port}"
    known = [f"{name} {line.split()[0]} {line.split()[1]}"
             for line in _read(tunnel["host-key-file"]).splitlines() if line.strip()]
    return (_file_lines(f"{REMOTE_TLS_DIR}/tunnel.key", _read(tunnel["key-file"]))
            + _file_lines(f"{REMOTE_TLS_DIR}/known_hosts", "\n".join(known)))


def _ssh(tunnel, *options):
    """ssh command (one shell line) for the tunnel to the workspace; forwards in "${fwd[@]}"."""
    dest = f"{tunnel.get('user', 'rsdat')}@{tunnel['host']}"
    return " ".join(["ssh", "-N", "-p", str(int(tunnel.get("port", 22))),
                     '-i "$RSDAT_TLS_DIR/tunnel.key"', "-o BatchMode=yes", "-o IdentitiesOnly=yes",
                     '-o UserKnownHostsFile="$RSDAT_TLS_DIR/known_hosts"',
                     "-o StrictHostKeyChecking=yes", "-o ExitOnForwardFailure=yes",
                     "-o ServerAliveInterval=15", "-o ServerAliveCountMax=4", *options,
                     '"${fwd[@]}"', shlex.quote(dest)]), dest


async def _job_state(job):
    """State of a Slurm job from squeue (ENDED when it's no longer listed)."""
    try:
        out = await job._call(["squeue", "--json"])
        jobs = json.loads(out).get("jobs", [])
    except Exception:                     # squeue through the REST API failed: keep waiting
        return "UNKNOWN"
    for j in jobs:
        if str(j.get("job_id")) == str(job.job_id):
            state = j.get("job_state")
            return (state[0] if isinstance(state, list) and state else state) or "UNKNOWN"
    return "ENDED"


_ALIVE = ("PENDING", "CONFIGURING", "RUNNING", "UNKNOWN")

# Slurm commands go through the REST API: without a timeout a wrong or unreachable API URL
# leaves a new cluster waiting silently
SLURM_CALL_TIMEOUT = 120
SLURM_UNKNOWN_LIMIT = 120          # seconds of failing squeue while waiting for a job


def _slurm_api_url():
    """The REST API URL the Slurm commands use (PROXY_SLURM_API_URL in their settings)."""
    url = None
    for path in ("/etc/rsdat/slurm.env", os.path.expanduser("~/.config/rsdat/slurm.env")):
        try:
            with open(path) as f:
                for line in f:
                    if line.startswith("PROXY_SLURM_API_URL="):
                        url = line.split("=", 1)[1].strip().strip('"')
        except OSError:
            pass
    return url or os.environ.get("PROXY_SLURM_API_URL") or "(not set)"


def _command_output(error):
    """What a failed Slurm command printed, from dask-jobqueue's error message."""
    text = str(error)
    # dask-jobqueue's message for a failed command: keep what the command itself printed
    for marker in ("stderr:\n", "stdout:\n"):
        if marker in text and text.split(marker, 1)[1].strip():
            text = text.split(marker, 1)[1].split("stderr:\n")[0]
            break
    return " ".join(l.strip() for l in text.splitlines() if l.strip())[:500]


def _slurm_hint(error):
    return (f"The Slurm REST API at {_slurm_api_url()} didn't work: {_command_output(error)}\n"
            "Check the token (rsdat-slurm-token --status) and the API URL (rsdat_slurm_api_url).")


def _refused_hint(job, error):
    return (f"Slurm refused the {job.job_name or 'Dask'} job: {_command_output(error)}\n"
            "Check the job settings: partition and account (rsdat_dask_queue, rsdat_dask_account, "
            "or queue=/account= for this cluster), cores, memory and walltime.")


async def _slurm_preflight():
    """Fail fast and clearly when the Slurm REST API can't be used (wrong URL, no or expired
    token), instead of waiting for a job that was never submitted."""
    try:
        await asyncio.wait_for(SLURMJob._call(["sinfo"]), SLURM_CALL_TIMEOUT)
    except asyncio.TimeoutError:
        raise RuntimeError(_slurm_hint(f"no answer within {SLURM_CALL_TIMEOUT} s")) from None
    except (RuntimeError, OSError) as e:
        raise RuntimeError(_slurm_hint(e)) from None


async def _submit(job):
    """job.start() (sbatch through the REST API) with a time limit and a clear error."""
    try:
        await asyncio.wait_for(job.start(), SLURM_CALL_TIMEOUT)
    except asyncio.TimeoutError:
        raise RuntimeError(_slurm_hint(f"sbatch didn't answer within {SLURM_CALL_TIMEOUT} s")) from None
    except RuntimeError as e:
        # the API answered (the cluster checked it first), so Slurm refused the job itself
        raise RuntimeError(_refused_hint(job, e)) from None


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
                                     + tls_lines + _tunnel_files(tunnel))
        self._command_template = self._scheduler_and_tunnel(
            python, remote_security, tunnel, tunnel_port, dashboard_port, cfg)

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
        ssh, dest = _ssh(tunnel)
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
            "    " + ssh + " &",
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
        await _submit(self.job)
        tls = self.security is not None and self.security.get_tls_config_for_role("client").get("cert")
        address = f"{'tls' if tls else 'tcp'}://127.0.0.1:{tunnel_port}"
        timeout = float(cfg.get("start-timeout", 900))
        deadline = time.monotonic() + timeout
        logger.info("Waiting for scheduler job %s (tunnel %s)", self.job.job_id, address)
        connection_args = self.security.get_connection_args("client") if self.security else {}
        unknown_since = None
        while True:
            try:
                async with rpc(address, timeout=5, connection_args=connection_args) as r:
                    identity = await r.identity()
                break
            except (OSError, asyncio.TimeoutError):
                state = await _job_state(self.job)
                unknown_since = (unknown_since or time.monotonic()) if state == "UNKNOWN" else None
                if unknown_since and time.monotonic() - unknown_since > SLURM_UNKNOWN_LIMIT:
                    await self.job.close()
                    raise RuntimeError(_slurm_hint(
                        f"squeue failed for {SLURM_UNKNOWN_LIMIT} s while waiting for scheduler job {self.job.job_id}"))
                if state not in _ALIVE:
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

    async def close(self):
        if self.job is not None:
            await self.job.close()
        await super().close()


# Runs in the gateway job (with the workers' Python): holds the job alive while the workspace
# process that submitted it keeps a "lease" connection open to it through the gateway. When
# that process ends in any way (also a crash or kill -9), its connection closes; after
# <idle> seconds without one, the listener exits and the job ends.
_LEASE_LISTENER = """
import select, socket, sys, time
port, idle = int(sys.argv[1]), float(sys.argv[2])
srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", port))
srv.listen()
conns, last = [], time.time()
while True:
    ready, _, _ = select.select([srv] + conns, [], [], 5)
    for c in ready:
        if c is srv:
            conns.append(srv.accept()[0])
        elif not c.recv(64):
            conns.remove(c)
            c.close()
    if conns:
        last = time.time()
    elif time.time() - last > idle:
        print(f"rsdat: no lease from the workspace for {idle:.0f}s, stopping", flush=True)
        sys.exit(0)
"""


class RSDATGatewayJob(SLURMJob):
    """Gateway job (rsdat.worker-route: gateway): opens a reverse dynamic SSH forward to this
    workspace, so 127.0.0.1:<socks_port> here is a SOCKS proxy that connects from the job's node
    to addresses in the worker network. Restarted when it drops; the job stops after
    rsdat.gateway.tunnel-give-up seconds without a tunnel, and after rsdat.gateway.idle-timeout
    seconds without a lease connection from this workspace (on the node's 127.0.0.1:<lease_port>,
    through the proxy)."""

    def __init__(self, socks_port, network, lease_port, worker_scheduler=None, security=None,
                 **kwargs):
        cfg = dask.config.get("rsdat.gateway", default={}) or {}
        tunnel = dask.config.get("rsdat.tunnel", default={}) or {}
        prologue = list(dask.config.get(f"jobqueue.{self.config_name}.job-script-prologue",
                                        default=None) or [])
        super().__init__(
            scheduler="tcp://gateway-placeholder:0",
            name="dask-gateway",
            job_name="dask-gateway",
            cores=cfg.get("cores", 1),
            processes=1,
            memory=cfg.get("memory", "1GiB"),
            walltime=cfg.get("walltime"),
            job_extra_directives=list(cfg.get("job-extra-directives") or []),
            job_script_prologue=prologue,
            **kwargs,
        )
        python = dask.config.get(f"jobqueue.{self.config_name}.python")
        self._job_script_prologue = (list(self._job_script_prologue) + _private_dir_lines()
                                     + _tunnel_files(tunnel)
                                     + _file_lines(f"{REMOTE_TLS_DIR}/lease.py", _LEASE_LISTENER))
        # Optionally a Dask worker in the same job, on the job's cores and memory (on clusters
        # that charge whole node shares anyway): the worker command of a normal worker job, for
        # the cluster's scheduler on the workspace
        worker = []
        if worker_scheduler:
            remote_security, tls_lines = ship_worker_security(security, private_dir=False)
            self._job_script_prologue += tls_lines
            # the worker gets less than the job, so it can't slow down the tunnel (ssh)
            helper = SLURMJob(scheduler=worker_scheduler_address(worker_scheduler),
                              name="rsdat-gateway", processes=1,
                              cores=cfg.get("worker-cores") or cfg.get("cores", 1),
                              memory=cfg.get("worker-memory") or cfg.get("memory", "1GiB"),
                              security=remote_security)
            worker = [helper._command_template + " &", "RSDAT_WORKER_PID=$!"]
        # (no PermitRemoteOpen: it only takes whole hosts or "*", not networks; this process
        # only sends connections to the worker network and the lease through the proxy)
        ssh, dest = _ssh(tunnel)
        give_up = int(cfg.get("tunnel-give-up", 600))
        idle = int(cfg.get("idle-timeout", 300))
        self._command_template = "\n".join([
            # the lease listener; the job ends when it does
            f'{python} "$RSDAT_TLS_DIR/lease.py" {int(lease_port)} {idle} &',
            "RSDAT_LEASE_PID=$!",
            "RSDAT_SSH_PID=",
            "RSDAT_WORKER_PID=",
            *worker,
            'trap \'kill $RSDAT_SSH_PID $RSDAT_LEASE_PID $RSDAT_WORKER_PID 2>/dev/null; rm -rf "$RSDAT_TLS_DIR"\' EXIT',
            # no destination after the port: OpenSSH's reverse dynamic forward (SOCKS)
            f'fwd=(-R "127.0.0.1:{int(socks_port)}")',
            f'echo "rsdat: gateway into {network} for {dest}, SOCKS on its 127.0.0.1:{int(socks_port)}"',
            "RSDAT_TUNNEL_OK=$(date +%s)",
            "while kill -0 $RSDAT_LEASE_PID 2>/dev/null; do",
            "    RSDAT_SSH_START=$(date +%s)",
            "    " + ssh + " &",
            "    RSDAT_SSH_PID=$!",
            "    wait -n $RSDAT_LEASE_PID $RSDAT_SSH_PID",
            "    kill -0 $RSDAT_LEASE_PID 2>/dev/null || break",
            "    now=$(date +%s)",
            "    [ $((now - RSDAT_SSH_START)) -ge 60 ] && RSDAT_TUNNEL_OK=$now",
            f"    if [ $((now - RSDAT_TUNNEL_OK)) -ge {give_up} ]; then",
            f'        echo "rsdat: no tunnel to {dest} for {give_up}s, stopping" >&2',
            "        exit 1",
            "    fi",
            '    echo "rsdat: tunnel closed, reconnecting" >&2',
            "    sleep 10",
            "done",
        ])


# --- connections from this process to the workers, through the gateway's SOCKS proxy ---

_ROUTES = {}            # ipaddress network -> SOCKS port on 127.0.0.1


def _socks_port_for(host):
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return None
    for net, port in _ROUTES.items():
        if ip in net:
            return port
    return None


async def _socks5_connect(stream, host, port):
    """SOCKS5 CONNECT (no authentication) to host:port over an open stream to the proxy."""
    await stream.write(b"\x05\x01\x00")
    if await stream.read_bytes(2) != b"\x05\x00":
        raise StreamClosedError(real_error=OSError("SOCKS proxy refused the greeting"))
    await stream.write(b"\x05\x01\x00\x01" + socket.inet_aton(host) + struct.pack(">H", port))
    ver, rep, _, atyp = await stream.read_bytes(4)
    if rep != 0:
        raise StreamClosedError(real_error=ConnectionRefusedError(
            f"gateway could not connect to {host}:{port} (SOCKS reply {rep})"))
    if atyp == 1:
        await stream.read_bytes(4 + 2)
    elif atyp == 4:
        await stream.read_bytes(16 + 2)
    else:
        (n,) = await stream.read_bytes(1)
        await stream.read_bytes(n + 2)


class _GatewayTCPClient(TCPClient):
    """tornado TCPClient for distributed's tcp:// and tls:// connectors: addresses in a routed
    network go through the SOCKS proxy (TLS on top, end to end), the rest directly."""

    async def connect(self, host, port, af=socket.AF_UNSPEC, ssl_options=None,
                      max_buffer_size=None, source_ip=None, source_port=None, timeout=None):
        socks = _socks_port_for(host)
        if socks is None:
            return await super().connect(host, port, af=af, ssl_options=ssl_options,
                                         max_buffer_size=max_buffer_size, source_ip=source_ip,
                                         source_port=source_port, timeout=timeout)
        stream = await super().connect("127.0.0.1", socks, max_buffer_size=max_buffer_size,
                                       timeout=timeout)
        try:
            await _socks5_connect(stream, host, int(port))
        except BaseException:
            stream.close()
            raise
        if ssl_options is not None:
            stream = await stream.start_tls(False, ssl_options=ssl_options)
        return stream


def _install_gateway_client():
    if not isinstance(_tcp.BaseTCPConnector.client, _GatewayTCPClient):
        _tcp.BaseTCPConnector.client = _GatewayTCPClient(resolver=_tcp._DefaultLoopResolver())


async def _socks_ok(port, timeout=5):
    """Is the gateway's SOCKS proxy answering on 127.0.0.1:port?"""
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), timeout)
    except (OSError, asyncio.TimeoutError):
        return False
    try:
        writer.write(b"\x05\x01\x00")
        await writer.drain()
        return await asyncio.wait_for(reader.readexactly(2), timeout) == b"\x05\x00"
    except (OSError, asyncio.TimeoutError, asyncio.IncompleteReadError):
        return False
    finally:
        writer.close()


def _open_lease(socks_port, lease_port, timeout=10):
    """Blocking: open the lease connection to the gateway job (its 127.0.0.1:lease_port,
    through the SOCKS proxy). None if it can't be opened (yet)."""
    try:
        sock = socket.create_connection(("127.0.0.1", socks_port), timeout=timeout)
    except OSError:
        return None
    try:
        sock.sendall(b"\x05\x01\x00")
        if sock.recv(2) != b"\x05\x00":
            raise OSError("SOCKS greeting refused")
        sock.sendall(b"\x05\x01\x00\x01" + socket.inet_aton("127.0.0.1")
                     + struct.pack(">H", lease_port))
        reply = sock.recv(10)
        if len(reply) < 2 or reply[1] != 0:
            raise OSError("lease listener not reachable")
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setblocking(False)
        return sock
    except OSError:
        sock.close()
        return None


def _lease_alive(sock):
    if sock is None:
        return False
    try:
        ready, _, _ = select.select([sock], [], [], 0)
        if ready and sock.recv(1, socket.MSG_PEEK) == b"":
            return False
    except OSError:
        return False
    return True


class _Gateway:
    """The gateway job of this process, shared by its clusters: submitted with the first
    cluster, resubmitted when it ends while clusters use it, cancelled with the last one."""

    def __init__(self):
        self._mutex = threading.Lock()
        self.users = 0
        self.job = None
        self.port = None
        self.network = None
        self._starting = False
        self._checking = False
        self.lease_port = None
        self._lease = None
        atexit.register(self._cancel_at_exit)

    async def acquire(self, network, worker_scheduler=None, security=None):
        with self._mutex:
            self.users += 1
            first = self.job is None and not self._starting
            if first:
                self._starting = True
        try:
            if first:
                self._worker = (worker_scheduler, security)
                try:
                    await self._start(network)
                finally:
                    self._starting = False
            else:
                while self._starting:
                    await asyncio.sleep(0.5)
                if self.job is None:
                    raise RuntimeError("the Dask gateway job failed to start")
        except BaseException:
            with self._mutex:
                self.users -= 1
            raise

    async def _start(self, network):
        cfg = dask.config.get("rsdat.gateway", default={}) or {}
        _install_gateway_client()
        self.network = ipaddress.ip_network(network, strict=False)
        self.port = self.port or _free_local_port()
        # a port for the lease listener on the job's node (random; collisions there are unlikely)
        self.lease_port = self.lease_port or _free_local_port()
        worker_scheduler, security = getattr(self, "_worker", (None, None))
        job = RSDATGatewayJob(self.port, network, self.lease_port,
                              worker_scheduler=worker_scheduler, security=security)
        await _submit(job)
        timeout = float(cfg.get("start-timeout", 900))
        deadline = time.monotonic() + timeout
        logger.info("Waiting for gateway job %s (SOCKS on 127.0.0.1:%s)", job.job_id, self.port)
        unknown_since = None
        while not await _socks_ok(self.port):
            state = await _job_state(job)
            unknown_since = (unknown_since or time.monotonic()) if state == "UNKNOWN" else None
            if unknown_since and time.monotonic() - unknown_since > SLURM_UNKNOWN_LIMIT:
                await job.close()
                raise RuntimeError(_slurm_hint(
                    f"squeue failed for {SLURM_UNKNOWN_LIMIT} s while waiting for gateway job {job.job_id}"))
            if state not in _ALIVE:
                raise RuntimeError(
                    f"Dask gateway job {job.job_id} ended ({state}) before its tunnel opened; "
                    "see dask-gateway-<job id>.log in the worker log directory on the cluster")
            if time.monotonic() > deadline:
                await job.close()
                raise RuntimeError(f"Dask gateway job {job.job_id} not reachable after "
                                   f"{timeout:.0f}s (state {state}); job cancelled")
            await asyncio.sleep(3)
        self.job = job
        _ROUTES[self.network] = self.port
        await self._renew_lease(deadline)
        logger.info("Gateway job %s: connections to %s go through 127.0.0.1:%s",
                    job.job_id, self.network, self.port)

    async def _renew_lease(self, deadline=None):
        """(Re)open the lease connection; the job ends without one (rsdat.gateway.idle-timeout)."""
        if _lease_alive(self._lease):
            return
        if self._lease is not None:
            self._lease.close()
        self._lease = None
        deadline = deadline or time.monotonic() + 60
        while self._lease is None and time.monotonic() < deadline:
            self._lease = await asyncio.get_running_loop().run_in_executor(
                None, _open_lease, self.port, self.lease_port)
            if self._lease is None:
                await asyncio.sleep(2)
        if self._lease is None:
            logger.warning("Could not open the lease to gateway job %s; it will stop after its "
                           "idle timeout unless the next check succeeds", self.job.job_id)

    def _cancel_at_exit(self):
        """At interpreter exit (JupyterLab or a kernel stopping without closing its clusters):
        cancel the gateway job now rather than after its idle timeout."""
        job = self.job
        if job is None or not job.job_id:
            return
        try:
            subprocess.run(["scancel", str(job.job_id)], timeout=20, capture_output=True)
        except Exception:
            pass                              # the lease timeout ends it anyway

    async def check(self):
        """Resubmit the gateway job if it ended while clusters still use it (e.g. walltime)."""
        with self._mutex:
            if self._checking or self._starting or self.job is None or self.users == 0:
                return
            self._checking = True
        try:
            if await _socks_ok(self.port):
                await self._renew_lease()
                return
            state = await _job_state(self.job)
            if state in _ALIVE:
                return
            logger.warning("Gateway job %s ended (%s); submitting a new one", self.job.job_id, state)
            old, self.job = self.job, None
            self._starting = True
            try:
                await self._start(str(self.network))
            except Exception:
                self.job = old          # try again at the next check
                logger.exception("Could not restart the Dask gateway job")
            finally:
                self._starting = False
        finally:
            self._checking = False

    async def release(self):
        with self._mutex:
            self.users -= 1
            last = self.users == 0
            job = self.job if last else None
            if last:
                self.job = None
                _ROUTES.pop(self.network, None)
                if self._lease is not None:
                    self._lease.close()
                    self._lease = None
        if job is not None:
            await job.close()


_GATEWAY = _Gateway()


class RSDATSLURMCluster(SLURMCluster):
    job_cls = RSDATSLURMJob

    def __init__(self, *args, security=None, **kwargs):
        if security is None and dask.config.get("distributed.comm.require-encryption", default=False):
            # the workspace's certificates from the dask config (distributed.comm.tls.*)
            security = Security()
        if scheduler_on_cluster():
            kwargs.setdefault("scheduler_cls", RSDATRemoteScheduler)
        self._gateway_network = (dask.config.get("rsdat.gateway.network", default=None)
                                 if uses_gateway() else None)
        if uses_gateway() and not self._gateway_network:
            raise ValueError("rsdat.worker-route is 'gateway' but rsdat.gateway.network is not set")
        self._gateway_acquired = False
        super().__init__(*args, security=security, **kwargs)

    async def _start(self):
        # every set-up submits jobs through the Slurm REST API: check it first
        await _slurm_preflight()
        if self._gateway_network and not self._gateway_acquired:
            # the scheduler (in this process) reaches the workers through the gateway job
            worker_scheduler = None
            if dask.config.get("rsdat.gateway.worker", default=False):
                # the scheduler isn't up yet, but its address is fixed: workers get the contact
                # host with the configured protocol and port
                port = dask.config.get(f"jobqueue.{self.job_cls.config_name}.scheduler-options.port",
                                       default=8786)
                proto = "tls" if self.security is not None and self.security.require_encryption else "tcp"
                worker_scheduler = f"{proto}://0.0.0.0:{int(port)}"
            await _GATEWAY.acquire(self._gateway_network, worker_scheduler, self.security)
            self._gateway_acquired = True
            self.periodic_callbacks["rsdat-gateway"] = PeriodicCallback(_GATEWAY.check, 30_000)
        try:
            await super()._start()
        except BaseException:
            await self._release_gateway()
            raise

    async def _close(self):
        try:
            await super()._close()
        finally:
            await self._release_gateway()

    async def _release_gateway(self):
        if self._gateway_acquired:
            self._gateway_acquired = False
            await _GATEWAY.release()

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

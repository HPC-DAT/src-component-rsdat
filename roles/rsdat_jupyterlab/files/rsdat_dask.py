"""Dask on Slurm for the RS-DAT workspace (installed by src-component-rsdat).

RSDATSLURMCluster is dask_jobqueue.SLURMCluster with three differences, all needed because the
scheduler runs on the workspace and the workers on a cluster that shares no filesystem or
private network with it:

1. TLS by default. When the dask config requires encryption (``distributed.comm.require-encryption``,
   set by the component), the cluster uses the workspace's certificates from the dask config
   (``distributed.comm.tls.*``). dask-jobqueue itself only passes certificate *file paths* to
   the workers, which assumes a shared filesystem; here the job script writes the certificates
   to the node's local scratch when the job starts (mode 600, removed when the job ends).

2. Contact host. Worker jobs connect to the scheduler at ``rsdat.scheduler-contact-host`` (with
   the scheduler's protocol and port), e.g. a public name for a workspace behind a
   floating/NAT address. distributed's own ``contact_address`` scheduler option has no effect
   (distributed 2026.8.0), and dask-jobqueue always gives workers the listening address.

3. Everything else (worker network, ports, local directory, ...) comes from /etc/dask/rsdat.yaml
   as for SLURMCluster.

    from rsdat_dask import RSDATSLURMCluster
    from distributed import Client
    cluster = RSDATSLURMCluster()
    cluster.scale(jobs=2)
    client = Client(cluster)       # or Client("tls://...") - TLS settings come from the config
"""
from urllib.parse import urlsplit

import dask
from dask_jobqueue import SLURMCluster
from dask_jobqueue.slurm import SLURMJob
from distributed.security import Security

__all__ = ["RSDATSLURMCluster", "RSDATSLURMJob", "worker_scheduler_address"]

# Directory on the node where the job writes the TLS files (expanded by the job's shell)
REMOTE_TLS_DIR = "$RSDAT_TLS_DIR"
_PEM_DELIMITER = "RSDAT_PEM_END"


def worker_scheduler_address(scheduler_address, contact_host=None):
    """The scheduler address to give the workers: same protocol and port, other host."""
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


def ship_worker_security(security):
    """Job-script lines that write the worker's TLS files on the node, and a Security object
    pointing at those files. (None, []) without TLS."""
    if security is None:
        return None, []
    worker = security.get_tls_config_for_role("worker")
    if not any(worker.get(k) for k in ("ca_file", "cert", "key")):
        return security, []

    lines = [
        'RSDAT_TLS_DIR=$(mktemp -d "${TMPDIR:-/tmp}/rsdat-tls.XXXXXX")',
        "trap 'rm -rf \"$RSDAT_TLS_DIR\"' EXIT",
    ]
    remote = {}
    for key, fname, attr in (("ca_file", "ca.pem", "tls_ca_file"),
                             ("cert", "worker.pem", "tls_worker_cert"),
                             ("key", "worker.key", "tls_worker_key")):
        value = worker.get(key)
        if not value:
            continue
        pem = _pem(value).strip("\n")
        if _PEM_DELIMITER in pem:
            raise ValueError("unexpected heredoc delimiter in certificate data")
        path = f"{REMOTE_TLS_DIR}/{fname}"
        lines += [f"(umask 077; cat > \"{path}\") <<'{_PEM_DELIMITER}'", *pem.splitlines(), _PEM_DELIMITER]
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


class RSDATSLURMCluster(SLURMCluster):
    job_cls = RSDATSLURMJob

    def __init__(self, *args, security=None, **kwargs):
        if security is None and dask.config.get("distributed.comm.require-encryption", default=False):
            # the workspace's certificates from the dask config (distributed.comm.tls.*)
            security = Security()
        super().__init__(*args, security=security, **kwargs)

    def _get_worker_security(self, security):
        # dask-jobqueue would dump in-memory credentials to a shared directory and pass paths;
        # RSDATSLURMJob ships the certificates in the job script instead.
        return security

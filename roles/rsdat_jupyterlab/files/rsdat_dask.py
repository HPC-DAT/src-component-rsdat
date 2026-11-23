"""Dask on Slurm for the RS-DAT workspace (installed by src-component-rsdat).

RSDATSLURMCluster is dask_jobqueue.SLURMCluster, except that worker jobs connect to the
scheduler at the host in the dask config key ``rsdat.scheduler-contact-host`` (with the
scheduler's actual port). This is needed when the workers must use another name or address
than the one the scheduler listens on, e.g. a workspace with a private IP behind a public
(floating/NAT) address. distributed's own ``contact_address`` option for the scheduler has no
effect (checked with distributed 2026.8.0), and dask-jobqueue always gives workers the
scheduler's listening address.

    from rsdat_dask import RSDATSLURMCluster
    cluster = RSDATSLURMCluster()        # defaults from /etc/dask/rsdat.yaml
    cluster.scale(jobs=2)
"""
from urllib.parse import urlsplit

import dask
from dask_jobqueue import SLURMCluster
from dask_jobqueue.slurm import SLURMJob

__all__ = ["RSDATSLURMCluster", "RSDATSLURMJob", "worker_scheduler_address"]


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


class RSDATSLURMJob(SLURMJob):
    def __init__(self, scheduler=None, name=None, **kwargs):
        super().__init__(scheduler=worker_scheduler_address(scheduler), name=name, **kwargs)


class RSDATSLURMCluster(SLURMCluster):
    job_cls = RSDATSLURMJob

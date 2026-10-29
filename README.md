# src-component-rsdat

SURF Research Cloud component (Ansible) for **RS-DAT** on an Ubuntu workspace:

- **JupyterLab** with the **Dask** extension (`dask-labextension`) and the **git** extension (`jupyterlab-git`)
- **Dask on Slurm**: the Dask scheduler runs on the workspace (in JupyterLab), the workers run as jobs on a remote Slurm cluster. Jobs are submitted and cancelled through the cluster's **Slurm REST API**, using the [SLURM-CLI-API-Proxy client](https://github.com/SLURM-CLI-API-Proxy/SLURM-CLI-API-Proxy-client) (`sbatch`, `squeue`, `scancel`, … over REST). The workspace needs no Slurm installation, no munge and no SSH to the cluster.
- **Prefect server** (UI and API) for workflows

JupyterLab and Prefect are published through **SRC-Nginx** with SRAM login, at `https://<workspace>/jupyter/` and `https://<workspace>/prefect/`.

```
 browser ──SRAM──▶ SRC-Nginx ──▶ JupyterLab (Dask scheduler) ──sbatch/scancel (REST)──▶ slurmrestd
                       │                ▲                                                   │
                       └──▶ Prefect     └──────────── Dask workers (Slurm jobs) ◀───────────┘
```

Status: prototype (RS-DAT catalogue item for HPC-DAT). Tested on Ubuntu 24.04 in a container, against a single-node Slurm 25.05 test cluster (see [Testing](#testing)). It has not yet been tested on a real Research Cloud workspace or a production cluster.

## Use as a Research Cloud component

1. Create an Ansible component in the Research Cloud portal pointing to this repository, playbook `rsdat.yml`.
2. In the catalogue item, put the **SRC-Nginx** component (and SRC-External/SRAM as usual) **before** this one.
3. Set the component parameters (below). The required ones are `rsdat_slurm_api_url` and `rsdat_dask_worker_python`.

Locally, on an Ubuntu machine with SRC-Nginx:

```bash
sudo ansible-playbook rsdat.yml \
  -e rsdat_slurm_api_url=https://slurm.example.org \
  -e rsdat_dask_worker_python=/home/me/venvs/dask/bin/python \
  -e rsdat_dask_queue=rome -e rsdat_dask_account=my-budget
```

## Parameters

| Parameter | Default | Meaning |
|---|---|---|
| `rsdat_slurm_api_url` | — (required) | Slurm REST API of the cluster, e.g. `https://slurm.snellius.surf.nl` |
| `rsdat_slurm_user`, `rsdat_slurm_jwt` | `""` | Optional Slurm user and token (`scontrol token lifespan=…`) for everyone on the workspace. **Secret.** Users can also set their own token, see below |
| `rsdat_dask_worker_python` | — (required) | Python **on the cluster** with the same `dask`/`distributed` version as `rsdat_dask_version` |
| `rsdat_dask_job_script_prologue` | `[]` | Shell lines run in each worker job before the worker starts, e.g. `["module load 2025", "source ~/venvs/dask/bin/activate"]` |
| `rsdat_dask_queue`, `rsdat_dask_account` | `""` | Slurm partition and account (budget) |
| `rsdat_dask_cores`, `rsdat_dask_processes`, `rsdat_dask_memory`, `rsdat_dask_walltime` | `1`, `1`, `4GiB`, `01:00:00` | Resources per worker job |
| `rsdat_dask_job_extra_directives` | `[]` | Extra `#SBATCH` options, e.g. `["--constraint=a100"]` |
| `rsdat_dask_log_directory`, `rsdat_dask_job_directory` | `""` | Directory on the cluster for worker logs, and the jobs' working directory |
| `rsdat_dask_scheduler_port` | `8786` | Port the workers use to reach the scheduler on the workspace |
| `rsdat_dask_contact_host` | `rsdat_fqdn` | Address the workers use to reach the workspace |
| `rsdat_fqdn` | the host's FQDN | Workspace host name |
| `rsdat_dask_version` | `2026.8.0` | dask/distributed version, which must match the workers' environment |
| `rsdat_slurm_proxy_pip_spec` | fork branch with the fixes | pip spec of the SLURM-CLI-API-Proxy client; pin to a tag or commit once merged upstream |
| `rsdat_extra_pip_packages` | `[]` | Extra Python packages for the notebooks |
| `rsdat_prefect_enabled` | `true` | Install and run the Prefect server |
| `rsdat_nginx_root_redirect` | `false` | Redirect `/` to JupyterLab (only if no other component serves `/`) |
| `rsdat_user` | `rsdat` | Unix user that runs JupyterLab and Prefect |

Other versions and settings are in `group_vars/all.yml` and `roles/*/defaults/main.yml`.

## Using it

**Slurm token.** The Slurm REST API needs a token. If none was set as a parameter, open a terminal in JupyterLab and run:

```bash
rsdat-slurm-token        # paste the output of 'scontrol token lifespan=86400' run on the cluster
```

The token is stored in `~/.config/rsdat/slurm.env` and read on every Slurm command, so nothing needs a restart. When it expires, run `rsdat-slurm-token` again.

**Dask.** In JupyterLab, open the Dask sidebar and click **New** to create a cluster, then **Scale** it. In a notebook:

```python
from dask_jobqueue import SLURMCluster
from distributed import Client

cluster = SLURMCluster()        # defaults from the component parameters (/etc/dask/rsdat.yaml)
cluster.scale(jobs=4)           # or cluster.adapt(maximum_jobs=10)
client = Client(cluster)
```

Any setting can be overridden per cluster, e.g. `SLURMCluster(cores=16, memory="32GiB", queue="genoa")`. `sbatch`, `squeue` and `scancel` also work in the JupyterLab terminal.

**Prefect.** The UI is at `https://<workspace>/prefect/`. Flows run from notebooks report to this server (`PREFECT_API_URL` is set in JupyterLab).

## Networking

Dask needs connections in both directions between the workspace and the worker nodes:

- workers → scheduler: compute nodes must reach `rsdat_dask_contact_host:rsdat_dask_scheduler_port` (TCP 8786 by default). The workspace's security group must allow this from the cluster's address range.
- scheduler (and notebook client) → workers: the workspace must reach the workers' ports on the compute nodes.

Compute nodes are often not reachable from outside the cluster, so check this per cluster before relying on it. If direct connections aren't possible, use a tunnel or run the scheduler on the cluster (not part of this component yet).

The workspace talks to the Slurm REST API over HTTPS only. It needs no SSH access to the cluster.

## Security notes

- JupyterLab runs as one user (`rsdat`) for the whole workspace. Its own login is disabled because SRC-Nginx does the SRAM login. It listens on `127.0.0.1` only. Everyone in the CO who can open the workspace shares this JupyterLab and its Slurm token. For per-user sessions, use JupyterHub instead (follow-up).
- A token given as `rsdat_slurm_jwt` is stored in `/etc/rsdat/slurm.env`, readable only by root and the `rsdat` group.

## Testing

`tests/run.sh` runs the playbook in an Ubuntu 24.04 container with systemd, with a mock SRC-Nginx that does what the SRAM check does. Dask workers run on a single-node Slurm 25.05 container: the `testing/slurm-container` image from the SLURM-CLI-API-Proxy client repo, branch `fix/install-scancel-dask`.

```bash
# once: build the Slurm test image (in a clone of the proxy client repo)
docker build -t slurm-cli-proxy-test testing/slurm-container

tests/run.sh             # KEEP=1 to keep the containers afterwards
```

The checks (`tests/check_workspace.sh`):
- The services run.
- JupyterLab is reachable through nginx and has the Dask and git extensions.
- The Prefect UI, its assets and the API work under `/prefect/`, and a flow runs against the server.
- `sbatch`, `squeue` and `scancel` work through the REST API.
- `SLURMCluster()` with the workspace defaults starts two workers on Slurm, computes a result and cancels the jobs on close.
- The Dask extension creates a cluster.
- A second playbook run changes nothing.

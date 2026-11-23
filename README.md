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
| `rsdat_dask_worker_network` | `""` | Network (CIDR) the workers listen on and advertise, e.g. `145.136.56.0/21` on Snellius. Each job picks its node's address in this network when it starts. See [Networking](#networking) |
| `rsdat_dask_worker_port`, `rsdat_dask_nanny_port` | `40000:40099`, `40100:40199` | Fixed port ranges for workers and nannies, so the cluster firewall can allow them (`""` = random) |
| `rsdat_dask_scheduler_port` | `8786` | Port the workers use to reach the scheduler on the workspace |
| `rsdat_dask_scheduler_host` | `0.0.0.0` | Address the scheduler listens on: all interfaces, or the workspace's public IP |
| `rsdat_dask_contact_host` | `rsdat_fqdn` | Host the workers use to reach the scheduler (`RSDATSLURMCluster` only; `""` = the scheduler's own address) |
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
from rsdat_dask import RSDATSLURMCluster
from distributed import Client

cluster = RSDATSLURMCluster()   # defaults from the component parameters (/etc/dask/rsdat.yaml)
cluster.scale(jobs=4)           # or cluster.adapt(maximum_jobs=10)
client = Client(cluster)
```

`RSDATSLURMCluster` is dask-jobqueue's `SLURMCluster` with one difference: workers reach the scheduler at `rsdat_dask_contact_host` (see [Networking](#networking)). The Dask sidebar uses it too. Plain `SLURMCluster()` gets the same defaults, but gives the workers the scheduler's own listening address.

Any setting can be overridden per cluster, e.g. `RSDATSLURMCluster(cores=16, memory="32GiB", queue="genoa")`. `sbatch`, `squeue` and `scancel` also work in the JupyterLab terminal.

**Prefect.** The UI is at `https://<workspace>/prefect/`. Flows run from notebooks report to this server (`PREFECT_API_URL` is set in JupyterLab).

## Networking

Dask connects in both directions between the scheduler (in JupyterLab on the workspace) and the workers (Slurm jobs on the compute nodes).

**Workers → scheduler.** Workers connect to `rsdat_dask_contact_host:rsdat_dask_scheduler_port` (TCP 8786 by default). The workspace's security group must allow this port from the compute nodes' addresses.
- The scheduler listens on `rsdat_dask_scheduler_host` (default all interfaces).
- If the workspace's public IP is on one of its interfaces, the defaults are enough.
- If the workspace has a private IP behind a floating/NAT address, set `rsdat_dask_contact_host` to the public name or IP. This needs `RSDATSLURMCluster`. dask-jobqueue always gives workers the scheduler's listening address, and distributed's `contact_address` option for the scheduler has no effect (checked with distributed 2026.8.0).

The worker's connection leaves the node via the node's routing, which Dask doesn't control. Check it on a compute node with `ip route get <workspace IP>`. On Snellius this goes out via the public VLAN (`src 145.136.x.x`).

**Scheduler → workers.** The scheduler, and the notebook client, connect to the address each worker advertises. That's the node's default-route address unless `rsdat_dask_worker_network` is set. Compute nodes often have several networks (on Snellius: internal Ethernet 172.18.x, InfiniBand 172.22.x, and the public VLAN 145.136.56.0/21). Only the public one is reachable from the workspace.
- With `rsdat_dask_worker_network: 145.136.56.0/21`, each job picks the node's address in that network when it starts and the worker listens on it (`--host`). The job stops with an error if the node has no such address.
- The address is picked by network rather than interface name, because names differ per node type. On Snellius it's `ens4f0np0.136` on CPU nodes and `eno1np0.136` on `gpu_a100` nodes.
- Workers and nannies use fixed port ranges (`40000:40099` and `40100:40199` by default). The cluster's firewall must allow the workspace to reach those ports on the worker network.

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
- `RSDATSLURMCluster()` with the workspace defaults starts two workers on Slurm. They connect through the contact host and listen in the worker network on the fixed port ranges. They compute a result and the jobs are cancelled on close.
- The Dask extension creates a cluster.
- A second playbook run changes nothing.

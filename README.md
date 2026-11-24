# src-component-rsdat

SURF Research Cloud component (Ansible) for **RS-DAT** on an Ubuntu workspace:

- **JupyterLab** with the **Dask** extension (`dask-labextension`) and the **git** extension (`jupyterlab-git`)
- **Dask on Slurm**: the Dask scheduler runs on the workspace (in JupyterLab), the workers run as jobs on a remote Slurm cluster. The workspace never has to connect to the compute nodes: a small gateway job opens an SSH tunnel back to the workspace, through which the scheduler reaches the workers (see [Networking](#networking)). Jobs are submitted and cancelled through the cluster's **Slurm REST API**, using the [SLURM-CLI-API-Proxy client](https://github.com/SLURM-CLI-API-Proxy/SLURM-CLI-API-Proxy-client) (`sbatch`, `squeue`, `scancel`, … over REST). The workspace needs no Slurm installation, no munge and no SSH access to the cluster.
- **Prefect server** (UI and API) for workflows

JupyterLab and Prefect are published through **SRC-Nginx** with SRAM login, at `https://<workspace>/jupyter/` and `https://<workspace>/prefect/`.

```
 browser ──SRAM──▶ SRC-Nginx ──▶ JupyterLab + Dask scheduler ──sbatch/scancel (REST)──▶ slurmrestd
                       │           │  ▲ :8786 (TLS)                                        │
                       └──▶ Prefect│  └──────────────────────── Dask workers (Slurm jobs) ◀─┘
                                   │ SOCKS 127.0.0.1:<port>          ▲ internal network
                     workspace sshd◀──── SSH tunnel (-R) ──── gateway job (Slurm job)
```

Status: prototype (RS-DAT catalogue item for HPC-DAT). Tested on Ubuntu 24.04 in a container, against a single-node Slurm 25.05 test cluster (see [Testing](#testing)). It has not yet been tested on a real Research Cloud workspace or a production cluster.

## Use as a Research Cloud component

1. Create an Ansible component in the Research Cloud portal pointing to this repository, playbook `rsdat.yml`.
2. In the catalogue item, put the **SRC-Nginx** component (and SRC-External/SRAM as usual) **before** this one.
3. Set the component parameters (below). The required ones are `rsdat_slurm_api_url` and the worker environment: `rsdat_dask_worker_image` or `rsdat_dask_worker_python`. On Snellius, also set `rsdat_dask_worker_network: 172.18.0.0/16` (internal network), and allow TCP 8786 into the workspace from the compute nodes' egress addresses.

Locally, on an Ubuntu machine with SRC-Nginx:

```bash
sudo ansible-playbook rsdat.yml \
  -e rsdat_slurm_api_url=https://slurm.example.org \
  -e rsdat_dask_worker_image=oras://ghcr.io/hpc-dat/src-component-rsdat-worker:2026.8.0 \
  -e rsdat_dask_worker_network=172.18.0.0/16 \
  -e rsdat_dask_queue=rome -e rsdat_dask_account=my-budget
```

## Parameters

| Parameter | Default | Meaning |
|---|---|---|
| `rsdat_slurm_api_url` | — (required) | Slurm REST API of the cluster, e.g. `https://slurm.snellius.surf.nl` |
| `rsdat_slurm_api_check` | `true` | Check at deployment that `rsdat_slurm_api_url` answers; a new Dask cluster also checks the API first and fails within seconds with a clear message |
| `rsdat_slurm_user`, `rsdat_slurm_jwt` | `""` | Optional Slurm user and token (`scontrol token lifespan=…`) for everyone on the workspace. **Secret.** Users can also set their own token, see below |
| `rsdat_dask_worker_image` | the image built from this checkout's `worker-image/` | Worker image (see [Worker environment](#worker-environment)). Default: `<rsdat_dask_worker_image_repo>:req-<hash of worker-image/>`, the image with exactly the workspace's packages. Or a fixed tag, or a `.sif` file on the cluster |
| `rsdat_dask_worker_image_repo` | `oras://ghcr.io/hpc-dat/src-component-rsdat-worker` | Where the workflow pushes the worker image |
| `rsdat_dask_worker_python` | `""` | Instead of an image: a Python **on the cluster** with exactly the versions in `worker-image/requirements.lock` and Python `worker-image/python-version` |
| `rsdat_dask_worker_apptainer_args` | `[]` | Extra `apptainer exec` options, e.g. `["--nv"]` for GPUs |
| `rsdat_dask_local_directory` | `${TMPDIR:-/tmp}` | Where workers spill data: the job's node-local scratch (expanded on the node), not the home filesystem |
| `rsdat_dask_job_script_prologue` | `[]` | Shell lines run in each worker job before the worker starts, e.g. `["module load 2025", "source ~/venvs/dask/bin/activate"]` |
| `rsdat_dask_queue`, `rsdat_dask_account` | `""` | Slurm partition and account (budget) |
| `rsdat_dask_cores`, `rsdat_dask_processes`, `rsdat_dask_memory`, `rsdat_dask_walltime` | `1`, `1`, `4GiB`, `01:00:00` | Resources per worker job |
| `rsdat_dask_job_extra_directives` | `[]` | Extra `#SBATCH` options, e.g. `["--constraint=a100"]` |
| `rsdat_dask_log_directory` | `$HOME/.rsdat/dask-logs` | Directory on the cluster for worker logs, one file per job. The job creates it, so nothing needs to be set up on the cluster |
| `rsdat_dask_job_directory` | `""` (Slurm user's home) | Working directory of the worker jobs on the cluster, created by the job if missing |
| `rsdat_dask_scheduler_location` | `workspace` | Where the Dask scheduler runs: `workspace` (in JupyterLab) or `cluster` (a Slurm job that opens an SSH tunnel to the workspace). See [Networking](#networking) |
| `rsdat_dask_worker_route` | `gateway` | Scheduler on the workspace: how it reaches the workers, `gateway` (through a gateway job's SSH tunnel) or `direct` |
| `rsdat_dask_gateway_cores`, `rsdat_dask_gateway_memory`, `rsdat_dask_gateway_walltime` | `16`, `28GiB`, `12:00:00` | Gateway job; partition and account as the workers. Resubmitted when it ends while clusters use it |
| `rsdat_dask_gateway_job_extra_directives` | `[]` | Extra `#SBATCH` options for the gateway job |
| `rsdat_dask_gateway_renew_before` | `1800` | Seconds before the gateway job's walltime that its replacement starts (at most a quarter of the walltime) |
| `rsdat_dask_gateway_worker_cores`, `rsdat_dask_gateway_worker_memory` | `14`, `24GiB` | Threads and memory limit of the gateway's worker: less than the job's, so it can't slow down the tunnel or push it out of memory |
| `rsdat_dask_gateway_worker` | `true` | Also run a Dask worker in the gateway job, on its cores and memory. The defaults (16 cores, 28 GiB) are the share Snellius charges for a job on a shared node anyway; elsewhere set them lower. It joins on top of `scale(jobs=N)` and stays until the gateway job ends |
| `rsdat_dask_gateway_idle_timeout` | `300` | Seconds after which a gateway job ends when the process that uses it is gone without closing its clusters |
| `rsdat_dask_worker_network` | `""` | Network (CIDR) the workers (and a scheduler on the cluster) listen on and advertise, e.g. `172.18.0.0/16` on Snellius. Each job picks its node's address in this network when it starts |
| `rsdat_dask_scheduler_cores`, `rsdat_dask_scheduler_memory`, `rsdat_dask_scheduler_walltime` | `1`, `2GiB`, `12:00:00` | Scheduler job (scheduler on the cluster); partition and account as the workers. The cluster ends with this job |
| `rsdat_dask_scheduler_job_extra_directives` | `[]` | Extra `#SBATCH` options for the scheduler job |
| `rsdat_dask_scheduler_start_timeout` | `900` | Seconds a new cluster waits for the scheduler or gateway job to start and its tunnel to open |
| `rsdat_dask_tunnel_host`, `rsdat_dask_tunnel_port` | `rsdat_fqdn`, `22` | SSH server on the workspace that the gateway's or scheduler's tunnel goes to |
| `rsdat_dask_tunnel_give_up` | `600` | The gateway or scheduler job stops after this many seconds without a tunnel |
| `rsdat_dask_worker_port`, `rsdat_dask_nanny_port` | `40000:40099`, `40100:40199` | Fixed port ranges for workers and nannies, so a firewall can allow them (`""` = random) |
| `rsdat_dask_scheduler_port` | `8786` | Scheduler on the workspace: port the workers use to reach it |
| `rsdat_dask_scheduler_host` | `0.0.0.0` | Scheduler on the workspace: address it listens on |
| `rsdat_dask_contact_host` | `rsdat_fqdn` | Scheduler on the workspace: host the workers use to reach it (`""` = the scheduler's own address) |
| `rsdat_dask_tls` | `true` | TLS between scheduler, workers and clients, with the workspace's own CA (see [Security notes](#security-notes)) |
| `rsdat_fqdn` | `workspace_fqdn`, else the VM's FQDN | Workspace host name. Research Cloud passes `workspace_fqdn` (e.g. `daskprefectsrv.advisorssrcdemo.src.surf-hosted.nl`). Set `rsdat_fqdn` to override |
| `rsdat_extra_pip_packages` | `[]` | Extra Python packages for the notebooks only, resolved on the workspace against the lock file, so they can't change a version the workers share. Packages the workers need too go in `worker-image/requirements.txt` (then run `worker-image/lock.sh`) |
| `rsdat_nginx_enabled` | `true` | Publish JupyterLab and Prefect through SRC-Nginx. `false` is for testing on a VM without SRC-Nginx only, see below |
| `rsdat_prefect_enabled` | `true` | Install and run the Prefect server |
| `rsdat_prefect_api_auth` | `true` | Protect the Prefect API with a generated password (`/etc/rsdat/prefect-api-password`) instead of SRAM, so Dask workers can report Prefect task states |
| `rsdat_prefect_api_url` | `https://<rsdat_fqdn>/prefect/api` | Prefect API for notebooks and Dask workers; without nginx `http://<rsdat_fqdn>:4200/api` |
| `rsdat_fqdn_resolve_locally` | `true` | Resolve `rsdat_fqdn` to `127.0.0.1` on the workspace (`/etc/hosts`) |
| `rsdat_nginx_root_redirect` | `true` | Redirect `https://<workspace>/` to JupyterLab; left out automatically if nginx doesn't accept it (another component already has `location = /`) |
| `rsdat_user` | `rsdat` | Unix user that runs JupyterLab and Prefect |

Package versions are in `worker-image/` (see [Worker environment](#worker-environment)); other settings are in `group_vars/all.yml` and `roles/*/defaults/main.yml`.

**Testing on a VM without SRC-Nginx.** Run the playbook with `-e rsdat_nginx_enabled=false`. JupyterLab and Prefect then listen on `127.0.0.1` only, and you reach them through an SSH tunnel:

```bash
ssh -L 8888:127.0.0.1:8888 -L 4200:127.0.0.1:4200 <user>@<workspace>
# http://localhost:8888/jupyter/   and   http://localhost:4200/prefect/
```

JupyterLab has no login of its own, so anyone with a shell on the VM can use it as `rsdat`. Don't use this for a shared workspace.

## Using it

**Start here.** Opening `/jupyter/` shows `RS-DAT-start-here.ipynb`: how the workspace works, a cell to store your Slurm token, a check against the cluster, a first Dask cluster, Prefect, and what to do when something fails. It's the user's own copy (never overwritten by the playbook; the original is in `/opt/rsdat/share`). New terminals show whether a Slurm token is set and when it expires. `rsdat_jupyterlab_open_start_here: false` keeps JupyterLab's own start page.

**Slurm token.** The Slurm REST API needs a token. If none was set as a parameter, open a terminal in JupyterLab and run:

```bash
rsdat-slurm-token        # paste the output of 'scontrol token lifespan=86400' run on the cluster
```

The token is stored in `~/.config/rsdat/slurm.env` and read on every Slurm command, so nothing needs a restart. `rsdat-slurm-token --status` shows for whom it is and when it expires; when it has, run `rsdat-slurm-token` again.

**Dask.** In JupyterLab, open the Dask sidebar and click **New** to create a cluster, then **Scale** it. In a notebook:

```python
from rsdat_dask import RSDATSLURMCluster
from distributed import Client

cluster = RSDATSLURMCluster()   # defaults from the component parameters (/etc/dask/rsdat.yaml)
cluster.scale(jobs=4)           # or cluster.adapt(maximum_jobs=10)
client = Client(cluster)
```

`RSDATSLURMCluster` is dask-jobqueue's `SLURMCluster`, adapted to a workspace that shares no network or filesystem with the cluster. The Dask sidebar uses it too. By default the scheduler runs in this JupyterLab and the first cluster also submits the gateway job, and waits until its tunnel is open (as long as the job waits in the queue). `cluster.close()` cancels the worker jobs, and the gateway job when no other cluster in the same process uses it. Plain `SLURMCluster()` doesn't work here: it doesn't send the certificates to the jobs and can't reach the workers.

Any setting can be overridden per cluster, e.g. `RSDATSLURMCluster(cores=16, memory="32GiB", queue="genoa")`. `sbatch`, `squeue` and `scancel` also work in the JupyterLab terminal.

**When a cluster doesn't start.** Creating a cluster first checks the Slurm REST API, then submits the gateway job: a wrong API URL or token, or a partition, account or resources that Slurm refuses, fail the creation with Slurm's reason, in the notebook and as the Dask sidebar's error. Worker jobs that Slurm refuses later, when scaling, don't fail the cluster: the reason is in the notebook's output (cluster made in a notebook) or in `journalctl -u rsdat-jupyterlab` (made in the sidebar), and the cluster just doesn't grow.

**Prefect.** The UI is at `https://<workspace>/prefect/`. Flows run from notebooks report to this server (`PREFECT_API_URL` is set in JupyterLab).

Prefect tasks can run on the Dask workers with `prefect_dask.DaskTaskRunner` (installed on the workspace and in the worker image):

```python
from prefect import flow, task
from prefect_dask import DaskTaskRunner

@task
def work(x):
    return x * 2

@flow(task_runner=DaskTaskRunner(address=cluster.scheduler_address))   # cluster = RSDATSLURMCluster()
def my_flow():
    return [f.result() for f in work.map(range(10))]
```

The tasks report their state to the Prefect API from the workers, so the API is reachable from the cluster: at `rsdat_prefect_api_url` (`https://<workspace>/prefect/api` behind SRC-Nginx), protected by Prefect's own password instead of SRAM (`rsdat_prefect_api_auth`). Notebooks get the URL and password in their environment (`PREFECT_API_URL`, `PREFECT_API_AUTH_STRING`), and Prefect passes them to the tasks with the flow's settings, inside the TLS-encrypted Dask traffic. The browser UI asks for the password once: the auth string `rsdat:<password>`, see `echo $PREFECT_API_AUTH_STRING` in a JupyterLab terminal. The workspace resolves its own public name to `127.0.0.1` (`/etc/hosts`, `rsdat_fqdn_resolve_locally`), so notebooks reach the API at the same URL as the workers.

Without SRC-Nginx (testing), the Prefect server listens on all interfaces and the URL is `http://<workspace>:4200/api`: that port must be open to the compute nodes, and the password travels unencrypted.

## Worker environment

Scheduler (workspace) and workers exchange functions and data with cloudpickle and msgpack, and Prefect tasks pass pydantic objects. Python and the versions of these packages must therefore be identical on both sides; a mismatch breaks in confusing ways. Both sides run Ubuntu 24.04's Python (the worker image is built on `ubuntu:24.04`), so Python and OpenSSL are the same (a uv-managed Python build with OpenSSL 3.5 on the workspace made about one in four test clusters lose their connection to the scheduler), and are installed from lock files with the exact version (and hash) of every package, also indirect ones:

| File in `worker-image/` | What it is |
|---|---|
| `requirements.txt` | What the workers need (edit): Dask, the serialization libraries, Prefect, … with exact versions |
| `workspace.in` | What the workspace needs on top (edit): JupyterLab, dask-jobqueue, the Dask and git extensions, the Slurm client |
| `python-version` | Ubuntu 24.04's Python version (`3.12.3`); the playbook and the image build stop if the OS Python differs |
| `workspace.lock` | Generated by `lock.sh` from `workspace.in`: what the playbook installs on the workspace (with [uv](https://docs.astral.sh/uv/), `uv pip sync`: exactly this, nothing else) |
| `requirements.lock` | Generated by `lock.sh` from `requirements.txt`, constrained to `workspace.lock`: what the worker image installs. Every package in it has the same version as in `workspace.lock`; `lock.sh` checks that |
| `worker.def` | Builds the worker image (Apptainer) |

After editing `requirements.txt`, `workspace.in` or `python-version`, run `worker-image/lock.sh` (needs uv: `pip install uv`) and commit the lock files; `worker-image/lock.sh --upgrade` moves unpinned packages to their newest versions. The workflow fails when the lock files are out of date.

`.github/workflows/worker-image.yml` builds the image on pushes to `main` that change `worker-image/` and on release tags (`v*`), checks that it has exactly the packages of `requirements.lock` and the Python of `python-version`, and pushes it to `oras://ghcr.io/<owner>/<repo>-worker` with these tags:

- `req-<hash>`: the first 12 characters of the SHA-256 of `requirements.lock` + `python-version` + `worker.def`. The playbook computes the same hash and uses this tag by default, so a workspace always gets the image with exactly its own packages, and nothing needs updating when the image is rebuilt. The image must have been built for that version of `worker-image/` (pushed to `main` or tagged); on a branch with changed requirements it doesn't exist yet.
- `<commit>` (7 characters): fixed, for pinning by hand.
- `<dask version>` and `latest`: moved by every build. It's based on [RS-DAT/JDS_development_template](https://github.com/RS-DAT/JDS_development_template). Unlike the template it installs this repo's lock file with uv instead of conda, so the image matches the workspace exactly.

With `rsdat_dask_worker_image` set, workers run in the image with the host's directories bound, as in the template. If compute nodes can't reach ghcr.io, pull the image on a login node first and point the parameter at the file:

```bash
apptainer pull ~/rsdat-worker.sif oras://ghcr.io/hpc-dat/src-component-rsdat-worker:2026.8.0
```

To add libraries that tasks use (e.g. xarray), add them to `worker-image/requirements.txt` and run `worker-image/lock.sh`. That updates both sides. In a notebook, `client.get_versions(check=True)` raises if the scheduler, workers and client differ.

## Networking

Dask needs connections in **both** directions between the scheduler and its workers, and between workers. If the scheduler can't reach the workers, tasks still run, but no result reaches the notebook (`future.result()` and `gather()` time out), and `client.run` and `client.restart` fail. This was tested with workers that can only connect out.

Each worker job runs two processes, each with its own port: the **nanny**, a small supervisor that starts the worker process and restarts it when it crashes or exceeds its memory limit, and the **worker** itself. The scheduler connects to both: to the worker for data and `client.run`, and to the nanny to restart or stop the worker.

Three set-ups, with `rsdat_dask_scheduler_location` and `rsdat_dask_worker_route`:

| | Scheduler | Scheduler → workers | Cluster firewall | Workspace firewall |
|---|---|---|---|---|
| **Gateway** (default) | in JupyterLab | through the gateway job's SSH tunnel | nothing | 8786 and SSH from the compute nodes |
| **Scheduler on the cluster** | Slurm job | inside the cluster | nothing | SSH from the compute nodes |
| **Direct** | in JupyterLab | directly | workspace → workers' ports | 8786 from the compute nodes |

None of them logs in to the cluster with SSH (which on Snellius needs MFA): the workspace only uses the Slurm REST API, and where SSH is used, a job on the cluster connects to the workspace.

### Gateway (default)

`rsdat_dask_scheduler_location: workspace`, `rsdat_dask_worker_route: gateway`. The scheduler runs in JupyterLab, so it doesn't wait in the queue, isn't limited by a walltime and uses no budget; only the workers are jobs.

- **Workers → scheduler:** directly, to `rsdat_dask_contact_host:rsdat_dask_scheduler_port` (TCP 8786, TLS). The workspace's security group must allow this port from the compute nodes' egress addresses.
- **Scheduler → workers:** the workers listen on the cluster's internal network (`rsdat_dask_worker_network`, `172.18.0.0/16` on Snellius), where they also reach each other. The workspace can't reach that network, so:
  1. With its first cluster, a JupyterLab process submits a **gateway job**: `ssh -N -R 127.0.0.1:<port> rsdat@<workspace>`, OpenSSH's reverse dynamic forward. On the workspace, `127.0.0.1:<port>` is then a SOCKS proxy that connects from the gateway's node.
  With `rsdat_dask_gateway_worker` the gateway job also runs a Dask worker for the cluster, on the job's cores and memory: the scheduler's address on the workspace is fixed (`tls://<rsdat_dask_contact_host>:8786`), so the job knows it before the scheduler starts.
  2. In that process, Dask's connections to addresses in the worker network go through the proxy; everything else connects directly. TLS runs end to end through it. No root, iptables or extra software on the workspace.
  3. The gateway is shared by the process's clusters and cancelled with the last cluster. Before its walltime (`rsdat_dask_gateway_walltime`) it is replaced without a gap: `rsdat_dask_gateway_renew_before` seconds earlier (30 minutes; at most a quarter of the walltime) a new gateway job starts on a new port; once its tunnel works, new connections to the workers go through it, the old gateway's worker is retired (Dask moves its data to other workers first) and the old job is cancelled. On Snellius that means two gateway jobs for up to 30 minutes every 12 hours. If a gateway job ends anyway (cancelled, node failure), a new one is submitted within 30 seconds. If the process ends without closing its clusters, it cancels the gateway at exit; after a crash or `kill -9`, the job ends by itself `rsdat_dask_gateway_idle_timeout` seconds later (default 300): the process holds a lease connection through the gateway to a small listener in the job, and the job stops when no lease has been open for that long. While it's being resubmitted, the scheduler can't reach the workers: results and restarts wait, but the workers stay registered (they connect to the scheduler themselves).
- Notebooks that connect with `Client("tls://…:8786")` from another kernel talk to the scheduler only, so they need no gateway. `client.run` and `gather` go through the scheduler too.
- Traffic between the scheduler and the workers (task results, data the notebook gathers) goes through one SSH connection on the gateway's node.

### Scheduler on the cluster

`rsdat_dask_scheduler_location: cluster`. The scheduler is a Slurm job next to the workers, so all Dask traffic stays inside the cluster. Only the notebook (and the Dask sidebar and dashboard) on the workspace needs the scheduler, and it reaches it through an SSH tunnel that the scheduler job opens to the workspace:

1. `RSDATSLURMCluster()` picks two free ports on the workspace's `127.0.0.1` and submits the scheduler job over REST.
2. The job starts the scheduler on the node's address in `rsdat_dask_worker_network`, then runs `ssh -N -R 127.0.0.1:<port>:<scheduler> -R 127.0.0.1:<port>:<dashboard> rsdat@<workspace>`. The tunnel is restarted when it drops.
3. The notebook connects to `tls://127.0.0.1:<port>`. Worker jobs get the scheduler's address in the cluster.

- The scheduler job uses `rsdat_dask_scheduler_cores`/`_memory`/`_walltime` in the workers' partition and account. When it ends, the workers stop too (after their death timeout), so give it the longest walltime you need. It also stops when it can't open the tunnel for `rsdat_dask_tunnel_give_up` seconds, e.g. when the workspace is gone.
- Creating a cluster waits for the scheduler job in the queue.

### Direct

`rsdat_dask_scheduler_location: workspace`, `rsdat_dask_worker_route: direct`. The scheduler runs in JupyterLab and connects to the workers itself. Besides 8786 into the workspace, the cluster firewall must allow the workspace to reach the workers' and nannies' ports (fixed ranges `40000:40099` and `40100:40199`) on `rsdat_dask_worker_network`. Doesn't work on Snellius: its firewall blocks the workspace from the compute nodes.

### Common to all

- The worker network is picked by network, not interface name, because names differ per node type. Each job uses the node's address in `rsdat_dask_worker_network`, and stops with an error if the node has none.
- On Snellius the public VLAN (145.136.56.0/21) only reaches the outside, not other compute nodes; use the internal network `172.18.0.0/16`. InfiniBand (172.22.0.0/16) should work as well but wasn't tested. Checked on Snellius: rome nodes reach each other by hostname (internal Ethernet) and reach a Research Cloud workspace on port 22, and workers reach a scheduler on the workspace on 8786.
- With the gateway or the scheduler on the cluster, the workspace's SSH server must accept key logins for `rsdat_user` from the compute nodes (`rsdat_dask_tunnel_host:rsdat_dask_tunnel_port`).
- If the workspace has a private IP behind a floating/NAT address, `rsdat_dask_contact_host` must be its public name or IP (default: `rsdat_fqdn`). dask-jobqueue always gives workers the scheduler's listening address, and distributed's `contact_address` option for the scheduler has no effect (checked with distributed 2026.8.0), hence `RSDATSLURMCluster`.

## Security notes

- **Dask uses TLS** (`rsdat_dask_tls: true`). Anyone who can open a connection to a Dask scheduler or worker can run code there: as `rsdat` on the workspace, and as your Slurm user on the compute nodes. The scheduler's and workers' ports are reachable from the cluster network, which every cluster user shares (and with the scheduler on the workspace, port 8786 there).
  - The playbook creates a private CA and one certificate for this workspace in `~rsdat/.config/rsdat/dask-tls` (mode 600; the CA key is deleted afterwards).
  - The Dask config requires TLS for every connection, so a connection without this certificate is refused.
  - `RSDATSLURMCluster` writes the certificate into the job script, because the workspace and the cluster share no filesystem. On the node, the job stores it in `$TMPDIR` (mode 600) and removes it when the job ends. Only you and the cluster admins can read the job script.
  - `Client("tls://…")` in notebooks, including the Dask sidebar's snippet, picks up the certificate from the Dask config. Plain `SLURMCluster()` doesn't send the certificate to the workers, so use `RSDATSLURMCluster`.
  - To create a new certificate, delete the directory, rerun the playbook and restart running clusters.

- **SSH tunnel key** (gateway, scheduler on the cluster). The playbook creates a key in `~rsdat/.config/rsdat/dask-tunnel` and allows it in `~rsdat/.ssh/authorized_keys` with `restrict,port-forwarding,permitlisten="127.0.0.1:*",command="/bin/false"`. Because `port-forwarding` also re-enables `-L`, `/etc/ssh/sshd_config.d/rsdat-dask-tunnel.conf` limits `rsdat` to remote forwarding (`AllowTcpForwarding remote`, `PermitListen 127.0.0.1:*`); the playbook checks with `sshd -T` that the SSH server applies it. So the key can only open listening ports on the workspace's `127.0.0.1`: no shell, no connections into the workspace or its network, no agent or X11. `rsdat` is meant only for this: don't use it for interactive SSH logins. The gateway or scheduler job gets the key, and the workspace's host key for `known_hosts`, in its job script, like the certificates. Someone with the key could open a port on the workspace's `127.0.0.1` and wait for connections; Dask clients only talk to it with the workspace's TLS certificate, which is in the same job script.

- **Gateway proxy.** The gateway's SOCKS proxy on the workspace's `127.0.0.1` has no authentication and connects to anything the gateway's node can reach (OpenSSH's `PermitRemoteOpen` can't be limited to a network). Dask only sends connections to the worker network through it, but any process on the workspace could use it to reach the cluster's network, as network traffic from that node, not as your Slurm user. Dask itself stays protected by TLS. On a workspace where others have a shell, use the scheduler on the cluster instead.

- JupyterLab runs as one user (`rsdat`) for the whole workspace. Its own login is disabled because SRC-Nginx does the SRAM login. It listens on `127.0.0.1` only. Everyone in the CO who can open the workspace shares this JupyterLab and its Slurm token. For per-user sessions, use JupyterHub instead (follow-up).
- **Prefect API.** With `rsdat_prefect_api_auth` the API at `/prefect/api/` is no longer behind the SRAM login but behind Prefect's password, so the Dask workers can reach it. Anyone with the password can read and change flow runs. Everyone who can open this JupyterLab can read it (`PREFECT_API_AUTH_STRING`). It's stored in `/etc/rsdat/prefect-api-password` (root) and in `/etc/rsdat/prefect-*.env` (root and the `rsdat` group).
- A token given as `rsdat_slurm_jwt` is stored in `/etc/rsdat/slurm.env`, readable only by root and the `rsdat` group.

## Testing

`tests/run.sh` runs the playbook in an Ubuntu 24.04 container with systemd, with a mock SRC-Nginx that does what the SRAM check does. Dask runs on a single-node Slurm 25.05 container: the `testing/slurm-container` image from the SLURM-CLI-API-Proxy client repo, branch `fix/install-scancel-dask`, plus an SSH client, iptables and bokeh (`tests/Dockerfile.slurm`, built by the test).

```bash
# once: build the Slurm test image (in a clone of the proxy client repo)
docker build -t slurm-cli-proxy-test testing/slurm-container

tests/run.sh                       # gateway (default); KEEP=1 to keep the containers
SCHEDULER=cluster tests/run.sh     # scheduler on the cluster
ROUTE=direct tests/run.sh          # scheduler in JupyterLab, direct connections
```

With the gateway or the scheduler on the cluster, the test blocks every connection from the workspace to the Slurm container except to slurmrestd, as the Snellius firewall does.

The checks:
- A second playbook run changes nothing.
- The services run.
- JupyterLab is reachable through nginx and has the Dask and git extensions.
- The Prefect UI, its assets and the API work under `/prefect/` (also without nginx), the API refuses requests without the password, and a flow runs against the server.
- A Prefect flow with `DaskTaskRunner` runs its tasks on the Slurm workers, and their states reach the Prefect server.
- `sbatch`, `squeue` and `scancel` work through the REST API.
- `RSDATSLURMCluster()` with the workspace defaults starts two workers on Slurm, over TLS, with the certificates shipped in the job script, listening in the worker network on the fixed port ranges.
  - Gateway: the workers connect through the contact host, and the scheduler reaches them through the gateway job.
  - Scheduler on the cluster: the scheduler job's tunnel answers on `127.0.0.1` of the workspace, and the workers get the scheduler's address in the cluster.
  - Direct: the workers connect through the contact host.
- The workers compute a result, `client.restart()` works (the scheduler reaches the nannies), the dashboard answers (through the tunnel when the scheduler is on the cluster), and all jobs, including the gateway, are cancelled on close.
- A client that only knows the scheduler address connects with TLS from the config, and a plain `tcp://` client is refused.
- Gateway: the gateway job ends when its process exits, or is killed with `-9`, without closing its cluster.
- The Dask extension creates a cluster and scales it to one worker.
- JupyterLab comes back after a restart.
- The jobs write their logs to `~/.rsdat/dask-logs` on the cluster and leave no certificates or keys on the node.
- The tunnel key can't open a shell, `-L` forwards or `-R` on other addresses than `127.0.0.1`.

Not covered: the worker image (the test cluster has no Apptainer; the workflow builds and checks the image), and real networks and SSH server settings of a Research Cloud workspace.

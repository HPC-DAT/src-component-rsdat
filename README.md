# src-component-rsdat

SURF Research Cloud component (Ansible) for **RS-DAT** on an Ubuntu workspace:

- **JupyterLab** with the **Dask** extension (`dask-labextension`) and the **git** extension (`jupyterlab-git`)
- **Dask on Slurm**: notebooks on the workspace use a Dask cluster whose scheduler and workers run as jobs on a remote Slurm cluster. The scheduler job opens an SSH tunnel back to the workspace, so the workspace never has to connect to the compute nodes (see [Networking](#networking)). Jobs are submitted and cancelled through the cluster's **Slurm REST API**, using the [SLURM-CLI-API-Proxy client](https://github.com/SLURM-CLI-API-Proxy/SLURM-CLI-API-Proxy-client) (`sbatch`, `squeue`, `scancel`, … over REST). The workspace needs no Slurm installation, no munge and no SSH access to the cluster.
- **Prefect server** (UI and API) for workflows

JupyterLab and Prefect are published through **SRC-Nginx** with SRAM login, at `https://<workspace>/jupyter/` and `https://<workspace>/prefect/`.

```
 browser ──SRAM──▶ SRC-Nginx ──▶ JupyterLab (Dask client) ──sbatch/scancel (REST)──▶ slurmrestd
                       │           ▲  127.0.0.1:<port>                                    │
                       └──▶ Prefect│                                                     ▼
                     workspace sshd◀──── SSH tunnel (-R) ──── Dask scheduler (Slurm job)
                                                                    ▲  internal network
                                                                    ▼
                                                              Dask workers (Slurm jobs)
```

Status: prototype (RS-DAT catalogue item for HPC-DAT). Tested on Ubuntu 24.04 in a container, against a single-node Slurm 25.05 test cluster (see [Testing](#testing)). It has not yet been tested on a real Research Cloud workspace or a production cluster.

## Use as a Research Cloud component

1. Create an Ansible component in the Research Cloud portal pointing to this repository, playbook `rsdat.yml`.
2. In the catalogue item, put the **SRC-Nginx** component (and SRC-External/SRAM as usual) **before** this one.
3. Set the component parameters (below). The required ones are `rsdat_slurm_api_url` and the worker environment: `rsdat_dask_worker_image` or `rsdat_dask_worker_python`. On Snellius, also set `rsdat_dask_worker_network: 172.18.0.0/16` (internal network).

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
| `rsdat_slurm_user`, `rsdat_slurm_jwt` | `""` | Optional Slurm user and token (`scontrol token lifespan=…`) for everyone on the workspace. **Secret.** Users can also set their own token, see below |
| `rsdat_dask_worker_image` | `""` | Worker image built from `worker-image/` (see [Worker environment](#worker-environment)): `oras://ghcr.io/hpc-dat/src-component-rsdat-worker:<tag>`, or a `.sif` file on the cluster. Set this **or** `rsdat_dask_worker_python` |
| `rsdat_dask_worker_python` | `""` | Instead of an image: a Python **on the cluster** with exactly the versions in `worker-image/requirements.txt` |
| `rsdat_dask_worker_apptainer_args` | `[]` | Extra `apptainer exec` options, e.g. `["--nv"]` for GPUs |
| `rsdat_dask_local_directory` | `${TMPDIR:-/tmp}` | Where workers spill data: the job's node-local scratch (expanded on the node), not the home filesystem |
| `rsdat_dask_job_script_prologue` | `[]` | Shell lines run in each worker job before the worker starts, e.g. `["module load 2025", "source ~/venvs/dask/bin/activate"]` |
| `rsdat_dask_queue`, `rsdat_dask_account` | `""` | Slurm partition and account (budget) |
| `rsdat_dask_cores`, `rsdat_dask_processes`, `rsdat_dask_memory`, `rsdat_dask_walltime` | `1`, `1`, `4GiB`, `01:00:00` | Resources per worker job |
| `rsdat_dask_job_extra_directives` | `[]` | Extra `#SBATCH` options, e.g. `["--constraint=a100"]` |
| `rsdat_dask_log_directory` | `$HOME/.rsdat/dask-logs` | Directory on the cluster for worker logs, one file per job. The job creates it, so nothing needs to be set up on the cluster |
| `rsdat_dask_job_directory` | `""` (Slurm user's home) | Working directory of the worker jobs on the cluster, created by the job if missing |
| `rsdat_dask_scheduler_location` | `cluster` | Where the Dask scheduler runs: `cluster` (a Slurm job that opens an SSH tunnel to the workspace) or `workspace` (in JupyterLab; needs connections in both directions). See [Networking](#networking) |
| `rsdat_dask_worker_network` | `""` | Network (CIDR) the workers (and a scheduler on the cluster) listen on and advertise, e.g. `172.18.0.0/16` on Snellius. Each job picks its node's address in this network when it starts |
| `rsdat_dask_scheduler_cores`, `rsdat_dask_scheduler_memory`, `rsdat_dask_scheduler_walltime` | `1`, `2GiB`, `12:00:00` | Scheduler job (scheduler on the cluster); partition and account as the workers. The cluster ends with this job |
| `rsdat_dask_scheduler_job_extra_directives` | `[]` | Extra `#SBATCH` options for the scheduler job |
| `rsdat_dask_scheduler_start_timeout` | `900` | Seconds a new cluster waits for the scheduler job to start and the tunnel to open |
| `rsdat_dask_tunnel_host`, `rsdat_dask_tunnel_port` | `rsdat_fqdn`, `22` | SSH server on the workspace that the scheduler job's tunnel goes to |
| `rsdat_dask_tunnel_give_up` | `600` | The scheduler job stops after this many seconds without a tunnel |
| `rsdat_dask_worker_port`, `rsdat_dask_nanny_port` | `40000:40099`, `40100:40199` | Fixed port ranges for workers and nannies, so a firewall can allow them (`""` = random) |
| `rsdat_dask_scheduler_port` | `8786` | Scheduler on the workspace: port the workers use to reach it |
| `rsdat_dask_scheduler_host` | `0.0.0.0` | Scheduler on the workspace: address it listens on |
| `rsdat_dask_contact_host` | `rsdat_fqdn` | Scheduler on the workspace: host the workers use to reach it (`""` = the scheduler's own address) |
| `rsdat_dask_tls` | `true` | TLS between scheduler, workers and clients, with the workspace's own CA (see [Security notes](#security-notes)) |
| `rsdat_fqdn` | `workspace_fqdn`, else the VM's FQDN | Workspace host name. Research Cloud passes `workspace_fqdn` (e.g. `daskprefectsrv.advisorssrcdemo.src.surf-hosted.nl`). Set `rsdat_fqdn` to override |
| `rsdat_dask_python_version` | `3.12` | Python minor version of workspace and workers; the playbook stops if the workspace's differs |
| `rsdat_slurm_proxy_pip_spec` | fork branch with the fixes | pip spec of the SLURM-CLI-API-Proxy client; pin to a tag or commit once merged upstream |
| `rsdat_extra_pip_packages` | `[]` | Extra Python packages for the notebooks only. Packages the workers need too go in `worker-image/requirements.txt` |
| `rsdat_nginx_enabled` | `true` | Publish JupyterLab and Prefect through SRC-Nginx. `false` is for testing on a VM without SRC-Nginx only, see below |
| `rsdat_prefect_enabled` | `true` | Install and run the Prefect server |
| `rsdat_nginx_root_redirect` | `false` | Redirect `/` to JupyterLab (only if no other component serves `/`) |
| `rsdat_user` | `rsdat` | Unix user that runs JupyterLab and Prefect |

Dask, distributed and the serialization libraries are pinned in `worker-image/requirements.txt`, which the workspace and the worker image both install. Other versions and settings are in `group_vars/all.yml` and `roles/*/defaults/main.yml`.

**Testing on a VM without SRC-Nginx.** Run the playbook with `-e rsdat_nginx_enabled=false`. JupyterLab and Prefect then listen on `127.0.0.1` only, and you reach them through an SSH tunnel:

```bash
ssh -L 8888:127.0.0.1:8888 -L 4200:127.0.0.1:4200 <user>@<workspace>
# http://localhost:8888/jupyter/   and   http://localhost:4200/prefect/
```

JupyterLab has no login of its own, so anyone with a shell on the VM can use it as `rsdat`. Don't use this for a shared workspace.

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

`RSDATSLURMCluster` is dask-jobqueue's `SLURMCluster`, adapted to a workspace that shares no network or filesystem with the cluster. The Dask sidebar uses it too. By default (`rsdat_dask_scheduler_location: cluster`) creating it submits the scheduler job and waits until the scheduler is reachable through the tunnel, which takes as long as the job waits in the queue. `cluster.scheduler_address` is then `tls://127.0.0.1:<port>` on the workspace, and `cluster.dashboard_link` points at the dashboard through JupyterLab. `cluster.close()` cancels the scheduler and worker jobs. Plain `SLURMCluster()` doesn't work here: it runs the scheduler on the workspace and doesn't send the certificates to the jobs.

Any setting can be overridden per cluster, e.g. `RSDATSLURMCluster(cores=16, memory="32GiB", queue="genoa")`. `sbatch`, `squeue` and `scancel` also work in the JupyterLab terminal.

**Prefect.** The UI is at `https://<workspace>/prefect/`. Flows run from notebooks report to this server (`PREFECT_API_URL` is set in JupyterLab).

## Worker environment

Scheduler (workspace) and workers exchange functions and data with cloudpickle and msgpack. Python's minor version and the versions of dask, distributed, cloudpickle, msgpack, tornado, toolz and numpy must therefore be identical on both sides. A mismatch breaks in confusing ways.

- `worker-image/requirements.txt` pins them. The playbook installs it on the workspace, and the worker image installs the same file.
- `worker-image/worker.def` builds the worker image: an Apptainer image on Python 3.12, the same as Ubuntu 24.04.
- `.github/workflows/worker-image.yml` builds it on changes to `worker-image/`, checks the versions and the Python version, and pushes it to `oras://ghcr.io/<owner>/<repo>-worker` with tags `<dask version>`, `<commit>` and `latest`. It's based on [RS-DAT/JDS_development_template](https://github.com/RS-DAT/JDS_development_template). Unlike the template it uses pip with this repo's exact pins instead of conda, so the image matches the workspace's pip environment.

With `rsdat_dask_worker_image` set, workers run in the image with the host's directories bound, as in the template. If compute nodes can't reach ghcr.io, pull the image on a login node first and point the parameter at the file:

```bash
apptainer pull ~/rsdat-worker.sif oras://ghcr.io/hpc-dat/src-component-rsdat-worker:2026.8.0
```

To add libraries that tasks use (e.g. xarray), add them with exact versions to `worker-image/requirements.txt`. That updates both sides. In a notebook, `client.get_versions(check=True)` raises if the scheduler, workers and client differ.

## Networking

Dask needs connections in **both** directions between the scheduler and its workers, and between workers. If the scheduler can't reach the workers, tasks still run, but no result reaches the notebook (`future.result()` and `gather()` time out), and `client.run` and `client.restart` fail. This was tested with workers that can only connect out.

Each worker job runs two processes, each with its own port: the **nanny**, a small supervisor that starts the worker process and restarts it when it crashes or exceeds its memory limit, and the **worker** itself. The scheduler connects to both: to the worker for data and `client.run`, and to the nanny to restart or stop the worker.

`rsdat_dask_scheduler_location` picks where the scheduler runs.

### Scheduler on the cluster (default)

The scheduler is a Slurm job next to the workers, so all Dask traffic stays inside the cluster. Only the notebook (and the Dask sidebar and dashboard) on the workspace needs the scheduler, and it reaches it through an SSH tunnel that the scheduler job opens to the workspace:

1. `RSDATSLURMCluster()` picks two free ports on the workspace's `127.0.0.1` and submits the scheduler job over REST.
2. The job starts the scheduler on the node's address in `rsdat_dask_worker_network`, then runs `ssh -N -R 127.0.0.1:<port>:<scheduler> -R 127.0.0.1:<port>:<dashboard> rsdat@<workspace>`. The tunnel is restarted when it drops.
3. The notebook connects to `tls://127.0.0.1:<port>`. Worker jobs get the scheduler's address in the cluster.

So the workspace only connects to the Slurm REST API, and the compute nodes only connect out to the workspace's SSH port (`rsdat_dask_tunnel_port`, 22). No firewall changes on the cluster side.
- Scheduler and workers must listen on a network the compute nodes share. On Snellius that's the internal network (`rsdat_dask_worker_network: 172.18.0.0/16`). The public VLAN (145.136.56.0/21) only reaches the outside, not other compute nodes. InfiniBand (172.22.0.0/16) should also work but wasn't tested.
- The scheduler job uses `rsdat_dask_scheduler_cores`/`_memory`/`_walltime` in the workers' partition and account. When it ends, the workers stop too (after their death timeout), so give it the longest walltime you need. It also stops when it can't open the tunnel for `rsdat_dask_tunnel_give_up` seconds, e.g. when the workspace is gone.
- The workspace's SSH server must accept key logins for `rsdat_user` from the compute nodes.
- Checked on Snellius: rome nodes reach each other by hostname (internal Ethernet) and reach a Research Cloud workspace on port 22.

### Scheduler on the workspace

The scheduler runs in JupyterLab (`rsdat_dask_scheduler_location: workspace`). This needs:

**Workers → scheduler.** Workers connect to `rsdat_dask_contact_host:rsdat_dask_scheduler_port` (TCP 8786 by default). The workspace's security group must allow this port from the compute nodes' addresses.
- The scheduler listens on `rsdat_dask_scheduler_host` (default all interfaces).
- If the workspace has a private IP behind a floating/NAT address, set `rsdat_dask_contact_host` to the public name or IP. dask-jobqueue always gives workers the scheduler's listening address, and distributed's `contact_address` option for the scheduler has no effect (checked with distributed 2026.8.0), hence `RSDATSLURMCluster`.

**Scheduler → workers.** The scheduler and the notebook client connect to the address each worker advertises: the node's address in `rsdat_dask_worker_network`, picked by network when the job starts (interface names differ per node type). Workers and nannies use fixed port ranges (`40000:40099` and `40100:40199`) that the cluster firewall must allow from the workspace. The workers must also reach each other on that network.

On Snellius this mode doesn't work: the firewall blocks the workspace from the compute nodes, and compute nodes can't reach each other over the public VLAN.

## Security notes

- **Dask uses TLS** (`rsdat_dask_tls: true`). Anyone who can open a connection to a Dask scheduler or worker can run code there: as `rsdat` on the workspace, and as your Slurm user on the compute nodes. The scheduler's and workers' ports are reachable from the cluster network, which every cluster user shares (and with the scheduler on the workspace, port 8786 there).
  - The playbook creates a private CA and one certificate for this workspace in `~rsdat/.config/rsdat/dask-tls` (mode 600; the CA key is deleted afterwards).
  - The Dask config requires TLS for every connection, so a connection without this certificate is refused.
  - `RSDATSLURMCluster` writes the certificate into the job script, because the workspace and the cluster share no filesystem. On the node, the job stores it in `$TMPDIR` (mode 600) and removes it when the job ends. Only you and the cluster admins can read the job script.
  - `Client("tls://…")` in notebooks, including the Dask sidebar's snippet, picks up the certificate from the Dask config. Plain `SLURMCluster()` doesn't send the certificate to the workers, so use `RSDATSLURMCluster`.
  - To create a new certificate, delete the directory, rerun the playbook and restart running clusters.

- **SSH tunnel key** (scheduler on the cluster). The playbook creates a key in `~rsdat/.config/rsdat/dask-tunnel` and allows it in `~rsdat/.ssh/authorized_keys` with `restrict,port-forwarding,permitlisten="127.0.0.1:*",command="/bin/false"`. Because `port-forwarding` also re-enables `-L`, `/etc/ssh/sshd_config.d/rsdat-dask-tunnel.conf` limits `rsdat` to remote forwarding (`AllowTcpForwarding remote`, `PermitListen 127.0.0.1:*`); the playbook checks with `sshd -T` that the SSH server applies it. So the key can only open listening ports on the workspace's `127.0.0.1`: no shell, no connections into the workspace or its network, no agent or X11. `rsdat` is meant only for this: don't use it for interactive SSH logins. The scheduler job gets the key, and the workspace's host key for `known_hosts`, in its job script, like the certificates. Someone with the key could open a port on the workspace's `127.0.0.1` and wait for connections; Dask clients only talk to it with the workspace's TLS certificate, which is in the same job script.

- JupyterLab runs as one user (`rsdat`) for the whole workspace. Its own login is disabled because SRC-Nginx does the SRAM login. It listens on `127.0.0.1` only. Everyone in the CO who can open the workspace shares this JupyterLab and its Slurm token. For per-user sessions, use JupyterHub instead (follow-up).
- A token given as `rsdat_slurm_jwt` is stored in `/etc/rsdat/slurm.env`, readable only by root and the `rsdat` group.

## Testing

`tests/run.sh` runs the playbook in an Ubuntu 24.04 container with systemd, with a mock SRC-Nginx that does what the SRAM check does. Dask runs on a single-node Slurm 25.05 container: the `testing/slurm-container` image from the SLURM-CLI-API-Proxy client repo, branch `fix/install-scancel-dask`, plus an SSH client, iptables and bokeh (`tests/Dockerfile.slurm`, built by the test).

```bash
# once: build the Slurm test image (in a clone of the proxy client repo)
docker build -t slurm-cli-proxy-test testing/slurm-container

tests/run.sh                       # scheduler on the cluster; KEEP=1 to keep the containers
SCHEDULER=workspace tests/run.sh   # scheduler in JupyterLab
```

With the scheduler on the cluster, the test blocks every connection from the workspace to the Slurm container except to slurmrestd, as the Snellius firewall does.

The checks:
- A second playbook run changes nothing.
- The services run.
- JupyterLab is reachable through nginx and has the Dask and git extensions.
- The Prefect UI, its assets and the API work under `/prefect/`, and a flow runs against the server.
- `sbatch`, `squeue` and `scancel` work through the REST API.
- `RSDATSLURMCluster()` with the workspace defaults starts two workers on Slurm, over TLS, with the certificates shipped in the job script, listening in the worker network on the fixed port ranges.
  - Scheduler on the cluster: the scheduler job's tunnel answers on `127.0.0.1` of the workspace, and the workers get the scheduler's address in the cluster.
  - Scheduler on the workspace: the workers connect through the contact host.
- The workers compute a result, `client.restart()` works (the scheduler reaches the nannies), the dashboard answers through the tunnel, and all jobs are cancelled on close.
- A client that only knows the scheduler address connects with TLS from the config, and a plain `tcp://` client is refused.
- The Dask extension creates a cluster and scales it to one worker.
- JupyterLab comes back after a restart.
- The jobs write their logs to `~/.rsdat/dask-logs` on the cluster and leave no certificates or keys on the node.

Not covered: the worker image (the test cluster has no Apptainer; the workflow builds and checks the image), and real networks and SSH server settings of a Research Cloud workspace.

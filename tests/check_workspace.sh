#!/bin/bash
# Checks on a workspace after rsdat.yml ran (run as root on the workspace).
# Uses nginx on localhost:80 (mock SRC-Nginx in tests) and a reachable Slurm REST API.
set -uo pipefail
RSDAT_USER="${RSDAT_USER:-rsdat}"
VENV="${VENV:-/opt/rsdat/venv}"
fail=0
check() { local name="$1"; shift; if "$@" >/tmp/check.out 2>&1; then echo "PASS $name"; else echo "FAIL $name"; sed 's/^/    /' /tmp/check.out | tail -20; fail=1; fi; }
as_user() { sudo -u "$RSDAT_USER" -i bash -lc "$*"; }

wait_http() { for _ in $(seq 60); do curl -fs -o /dev/null "$1" && return 0; sleep 2; done; return 1; }

check "services active" systemctl is-active rsdat-jupyterlab rsdat-prefect nginx
check "JupyterLab via nginx (/jupyter/api/status)" wait_http http://localhost/jupyter/api/status
check "JupyterLab UI page" bash -c 'curl -fsL http://localhost/jupyter/lab | grep -q "JupyterLab"'
check "Dask and git lab extensions installed" bash -c "$VENV/bin/jupyter labextension list 2>&1 | grep -q dask-labextension && $VENV/bin/jupyter labextension list 2>&1 | grep -q '@jupyterlab/git'"
check "jupyter-server-proxy enabled" bash -c "$VENV/bin/jupyter server extension list 2>&1 | grep -q 'jupyter_server_proxy.*enabled'"
check "Prefect API via nginx (/prefect/api/health)" bash -c 'wait_http() { for _ in $(seq 90); do curl -fs "$1" | grep -q true && return 0; sleep 2; done; return 1; }; wait_http http://localhost/prefect/api/health'
check "Prefect UI and assets under /prefect/" bash -c '
    html=$(curl -fsL http://localhost/prefect/) || exit 1
    asset=$(echo "$html" | grep -oE "(src|href)=\"/prefect/[^\"]+\.(js|css)\"" | head -1 | cut -d\" -f2)
    [ -n "$asset" ] || { echo "no /prefect/ assets in UI page"; exit 1; }
    curl -fs -o /dev/null "http://localhost$asset"'
# (the UI build must be in the service user's directory: the environment is root's)
check "Prefect UI served by the server itself under /prefect/, built in ~rsdat/.prefect" bash -c 'curl -fs http://127.0.0.1:4200/prefect/ | grep -q "/prefect/" && test -d /home/rsdat/.prefect/ui-build'
cat > /tmp/prefect_check.py <<'PY'
from prefect import flow

@flow
def hello():
    return 42

assert hello() == 42
print("flow ran against", __import__("prefect").settings.PREFECT_API_URL.value())
PY
chmod 644 /tmp/prefect_check.py
PREFECT_ENV="set -a; . /etc/rsdat/prefect-client.env; set +a"
check "Prefect machine API (/prefect/machine-api/) refuses requests without the password" bash -c '[ "$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "{}" http://localhost/prefect/machine-api/flows/filter)" = 401 ]'
check "Prefect machine API accepts the password" bash -c '. /etc/rsdat/prefect-client.env; [ "$(curl -s -o /dev/null -w "%{http_code}" -u "$PREFECT_API_AUTH_STRING" -X POST -H "Content-Type: application/json" -d "{}" http://localhost/prefect/machine-api/flows/filter)" = 200 ]'
check "Prefect API for the browser (/prefect/api/): after the SRAM login without the password (nginx adds it)" bash -c '[ "$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "{}" http://localhost/prefect/api/flows/filter)" = 200 ]'
check "Prefect UI doesn't ask for the password behind the SRAM login" bash -c 'curl -fs http://localhost/prefect/ui-settings | tee /dev/stderr | grep -q "\"auth\":null" && curl -fs http://127.0.0.1:4200/prefect/ui-settings | grep -q "\"auth\":\"BASIC\""'
check "Prefect flow runs against the server (JupyterLab environment)" as_user "$PREFECT_ENV; $VENV/bin/python /tmp/prefect_check.py"
check "Start-here notebook in the home directory, opened on /jupyter/" bash -c 'test -s /home/rsdat/RS-DAT-start-here.ipynb && curl -s -o /dev/null -w "%{redirect_url}" http://localhost/jupyter/ | grep -q "lab/tree/RS-DAT-start-here.ipynb"'
check "New terminals show the Slurm token's state" as_user 'script -qc "bash -li -c exit" /dev/null | grep -q "Slurm token for testuser, valid until"'
check "Workspace token's user in slurm.env" grep -qx "SLURM_USER=testuser" /etc/rsdat/slurm.env
check "Slurm commands on PATH" as_user 'test "$(command -v sbatch)" = /opt/rsdat/slurm-bin/sbatch && squeue >/dev/null'
cat > /tmp/sbatch_check.sh <<'SH'
set -e
printf '#!/bin/bash\n#SBATCH -J rsdat-check\n#SBATCH -t 00:05:00\nsleep 60\n' > /tmp/rsdat-check-job.sh
id=$(sbatch --parsable -D /tmp /tmp/rsdat-check-job.sh)
echo "job $id"
squeue | grep -q "$id"
scancel "$id"
SH
chmod 644 /tmp/sbatch_check.sh
check "sbatch/squeue/scancel (through the REST API)" as_user "bash /tmp/sbatch_check.sh"

# Dask cluster with the workspace defaults (/etc/dask/rsdat.yaml), as in a notebook
cat > /tmp/dask_check.py <<'PY'
import ipaddress, os, urllib.request
from rsdat_dask import RSDATSLURMCluster
from distributed import Client

on_cluster = os.environ.get("RSDAT_EXPECT_SCHEDULER") == "cluster"
net = os.environ.get("RSDAT_EXPECT_WORKER_NET")

def in_net(addr):
    host, port = addr.removeprefix("tls://").rsplit(":", 1)
    assert ipaddress.ip_address(host) in ipaddress.ip_network(net), f"{addr} not in {net}"
    return int(port)

cluster = RSDATSLURMCluster()     # all settings from /etc/dask/rsdat.yaml
script = cluster.job_script()
print("\n".join(l for l in script.splitlines() if "PRIVATE KEY" not in l))
print("scheduler:", cluster.scheduler_address)
assert cluster.scheduler_address.startswith("tls://"), "TLS should be on by default"
assert script.count("-----BEGIN") == 3, "certificates should be in the job script"
if on_cluster:
    # scheduler job on the cluster: this workspace reaches it through the tunnel on 127.0.0.1,
    # the workers at its address in the cluster
    assert cluster.scheduler_address.startswith("tls://127.0.0.1:"), cluster.scheduler_address
    contact = cluster.scheduler.contact_address
    print("scheduler in the cluster:", contact)
    if net:
        in_net(contact)
    assert contact in script, "workers don't get the scheduler's address in the cluster"
else:
    contact = os.environ.get("RSDAT_EXPECT_CONTACT_HOST")
    if contact:
        assert f"tls://{contact}:8786" in script, "workers don't get the contact host"
cluster.scale(jobs=2)
with Client(cluster) as client:
    client.wait_for_workers(2, timeout=240)
    total = client.submit(sum, client.map(lambda x: x * x, range(100))).result(timeout=60)
    assert total == sum(x * x for x in range(100)), total
    workers = client.scheduler_info()["workers"]
    print("workers:", list(workers))
    if net:
        for addr, info in workers.items():
            assert addr.startswith("tls://"), addr
            port = in_net(addr)
            assert 40000 <= port <= 40099, f"worker port {port} outside 40000:40099"
            assert info["nanny"].rsplit(":", 1)[1].startswith("401"), info["nanny"]
    if not on_cluster and os.environ.get("RSDAT_EXPECT_ROUTE") == "gateway":
        names = [info["name"] for info in workers.values()]
        assert any(n.startswith("rsdat-gateway") for n in names), f"no worker in the gateway job: {names}"
    # the scheduler can reach the nannies (restart) and results reach this notebook
    client.restart()
    client.wait_for_workers(2, timeout=120)
    assert client.submit(lambda: 7).result(timeout=60) == 7
    print("dashboard:", cluster.dashboard_link)
    if on_cluster:
        port = cluster.scheduler.dashboard_port
        assert port and f"/proxy/{port}/" in cluster.dashboard_link, cluster.dashboard_link
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/status", timeout=20) as r:
            assert r.status == 200, r.status
    # a client that only knows the address (the Dask sidebar's snippet) uses the TLS config
    with Client(cluster.scheduler_address, timeout=20) as c2:
        assert c2.submit(lambda: 42).result(timeout=60) == 42
    # plain tcp is refused
    try:
        Client(cluster.scheduler_address.replace("tls://", "tcp://"), timeout=5)
        raise AssertionError("plain tcp:// client was accepted")
    except (OSError, RuntimeError, TimeoutError):
        pass
cluster.close()
print("OK")
PY
chmod 644 /tmp/dask_check.py
check "RSDATSLURMCluster with workspace defaults (scheduler on the ${RSDAT_EXPECT_SCHEDULER:-workspace}$([ "${RSDAT_EXPECT_SCHEDULER:-workspace}" = workspace ] && echo ", route ${RSDAT_EXPECT_ROUTE:-direct}"), 2 workers, worker network, restart, dashboard)" as_user "export PATH=/opt/rsdat/slurm-bin:$VENV/bin:\$PATH RSDAT_EXPECT_WORKER_NET=${RSDAT_EXPECT_WORKER_NET:-} RSDAT_EXPECT_CONTACT_HOST=${RSDAT_EXPECT_CONTACT_HOST:-} RSDAT_EXPECT_SCHEDULER=${RSDAT_EXPECT_SCHEDULER:-}; cd ~ && timeout 420 python /tmp/dask_check.py"
check "Dask jobs (workers, scheduler/gateway) cancelled after close" as_user 'sleep 5; ! squeue | grep -q " R .*dask-\| PD .*dask-"'

# Prefect tasks on the Dask workers on Slurm (prefect_dask.DaskTaskRunner): they report to
# the Prefect API at rsdat_prefect_api_url with the password from the flow's settings
cat > /tmp/prefect_dask_check.py <<'PY'
import asyncio, socket
from prefect import flow, task, get_client
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId
from prefect_dask import DaskTaskRunner
from rsdat_dask import RSDATSLURMCluster

cluster = RSDATSLURMCluster()
cluster.scale(jobs=1)
cluster.wait_for_workers(1, timeout=300)

@task
def where(x):
    return x, socket.gethostname()

@flow(task_runner=DaskTaskRunner(address=cluster.scheduler_address))
def on_workers(n=4):
    return [f.result() for f in where.map(range(n))]

state = on_workers(return_state=True)
result = state.result()
print("results:", result)
assert [x for x, _ in result] == list(range(4)), result
assert all(h != socket.gethostname() for _, h in result), "tasks didn't run on the workers"

async def task_states():
    async with get_client() as c:
        runs = await c.read_task_runs(flow_run_filter=FlowRunFilter(
            id=FlowRunFilterId(any_=[state.state_details.flow_run_id])))
        return [r.state.type.value for r in runs]
states = asyncio.run(task_states())
print("task run states on the server:", states)
assert len(states) == 4 and set(states) == {"COMPLETED"}, states
cluster.close()
print("OK")
PY
chmod 644 /tmp/prefect_dask_check.py
check "Prefect flow with DaskTaskRunner: tasks on the Slurm workers, states on the server" as_user "export PATH=/opt/rsdat/slurm-bin:$VENV/bin:\$PATH; $PREFECT_ENV; cd ~ && timeout 600 python /tmp/prefect_dask_check.py"

# A wrong Slurm REST API URL: creating a cluster fails quickly with a message naming the API
cat > /tmp/wrong_url_check.sh <<'SH'
set -e
mkdir -p ~/.config/rsdat
cp ~/.config/rsdat/slurm.env /tmp/slurm.env.bak 2>/dev/null || true
echo "PROXY_SLURM_API_URL=http://127.0.0.1:9" >> ~/.config/rsdat/slurm.env
start=$(date +%s)
out=$(timeout 200 python -c 'from rsdat_dask import RSDATSLURMCluster; RSDATSLURMCluster()' 2>&1 || true)
if [ -f /tmp/slurm.env.bak ]; then cp /tmp/slurm.env.bak ~/.config/rsdat/slurm.env; else rm -f ~/.config/rsdat/slurm.env; fi
echo "$out" | tail -3
echo "$out" | grep -q "Slurm REST API at http://127.0.0.1:9" && [ $(( $(date +%s) - start )) -lt 150 ]
SH
chmod 644 /tmp/wrong_url_check.sh
check "Wrong Slurm REST API URL: a new cluster fails quickly with a clear message" as_user "export PATH=/opt/rsdat/slurm-bin:$VENV/bin:\$PATH; cd ~ && bash /tmp/wrong_url_check.sh"

# Gateway renewal: a gateway job with a 4-minute walltime is replaced a minute before it ends
# (a quarter of its walltime), while the cluster keeps working: results keep coming back and
# data on the old gateway's worker survives
if [ "${RSDAT_EXPECT_SCHEDULER:-workspace}" = workspace ] && [ "${RSDAT_EXPECT_ROUTE:-direct}" = gateway ]; then
cat > /tmp/gateway_renew.py <<'PY'
import asyncio, time
from distributed import Client
from rsdat_dask import RSDATSLURMCluster, _GATEWAY, _job_state

cluster = RSDATSLURMCluster()
first = _GATEWAY.job
cluster.scale(jobs=1)
client = Client(cluster)
client.wait_for_workers(2, timeout=300)
# data on every worker, including the old gateway's
data = client.scatter(list(range(8)), broadcast=False)
t0, worst = time.time(), 0.0
while time.time() - t0 < 330 and _GATEWAY.job is first:
    t = time.time()
    assert client.submit(lambda: 41 + 1).result(timeout=60) == 42
    worst = max(worst, time.time() - t)
    time.sleep(5)
assert _GATEWAY.job is not first, "gateway job not replaced before its walltime"
print("replaced", first.job_id, "by", _GATEWAY.job.job_id, "after", int(time.time() - t0), "s; slowest result", round(worst, 1), "s")
for _ in range(30):
    t = time.time()
    assert client.submit(lambda: 7).result(timeout=60) == 7
    worst = max(worst, time.time() - t)
    if asyncio.run(_job_state(first)) not in ("RUNNING", "PENDING"):
        break
    time.sleep(2)
state = asyncio.run(_job_state(first))
assert state not in ("RUNNING", "PENDING", "TIMEOUT"), f"old gateway job {first.job_id}: {state}"
assert sum(client.gather(data)) == sum(range(8)), "data lost in the renewal"
assert worst < 30, f"a result took {worst:.0f} s during the renewal"
names = [w["name"] for w in client.scheduler_info()["workers"].values()]
assert "rsdat-gateway-1" not in names and any(n.startswith("rsdat-gateway-") for n in names), names
cluster.close()
print("OK", state, names)
PY
chmod 644 /tmp/gateway_renew.py
check "Gateway job replaced before its walltime without a gap" as_user "export PATH=/opt/rsdat/slurm-bin:$VENV/bin:\$PATH DASK_RSDAT__GATEWAY__WALLTIME=00:04:00; cd ~ && timeout 600 python /tmp/gateway_renew.py"
fi

# Gateway: a process that ends without closing its cluster doesn't leave the gateway job behind
if [ "${RSDAT_EXPECT_SCHEDULER:-workspace}" = workspace ] && [ "${RSDAT_EXPECT_ROUTE:-direct}" = gateway ]; then
cat > /tmp/gateway_orphan.py <<'PY'
import os, sys, time
from rsdat_dask import RSDATSLURMCluster, _GATEWAY
cluster = RSDATSLURMCluster()
print(_GATEWAY.job.job_id, flush=True)
if sys.argv[1] == "exit":
    sys.exit(0)                                # normal exit, cluster not closed: atexit cancels
time.sleep(3600)                               # killed with -9 by the check: the lease ends it
PY
cat > /tmp/gateway_orphan.sh <<'SH'
set -e
# (cancelled jobs stay listed for a while, with state CA)
gone() { for _ in $(seq "$2"); do squeue | awk -v id="$1" '$1 == id && ($5 == "R" || $5 == "PD") {f = 1} END {exit !f}' || return 0; sleep 2; done; return 1; }
id=$(timeout 300 python /tmp/gateway_orphan.py exit | tail -1)
echo "exit: gateway job $id"; gone "$id" 15 || { echo "gateway $id still there after a normal exit"; exit 1; }
python /tmp/gateway_orphan.py kill > /tmp/orphan.out & pid=$!
for _ in $(seq 150); do [ -s /tmp/orphan.out ] && break; sleep 2; done
id=$(tail -1 /tmp/orphan.out); kill -9 $pid
echo "kill -9: gateway job $id"; gone "$id" $(( (RSDAT_GATEWAY_IDLE + 60) / 2 )) || { echo "gateway $id still there after kill -9"; exit 1; }
SH
chmod 644 /tmp/gateway_orphan.py /tmp/gateway_orphan.sh
check "Gateway job ends when its process exits or is killed without closing the cluster" as_user "export PATH=/opt/rsdat/slurm-bin:$VENV/bin:\$PATH RSDAT_GATEWAY_IDLE=${RSDAT_GATEWAY_IDLE:-300}; cd ~ && timeout 600 bash /tmp/gateway_orphan.sh"
fi

# Dask extension: create a cluster through the JupyterLab extension API (the "New" button)
cat > /tmp/labext_check.sh <<'SH'
set -e
api=http://localhost/jupyter/dask/clusters
curl -fs -c /tmp/jar -o /dev/null http://localhost/jupyter/lab
xsrf=$(awk '$6 == "_xsrf" {print $7}' /tmp/jar)
req() { curl -fs -b /tmp/jar -H "X-XSRFToken: $xsrf" "$@"; }
# "New" button: create a cluster with the configured factory (SLURMCluster)
id=$(req -X PUT "$api" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
echo "cluster $id"
# "Scale" button: one worker, then wait until it has joined
req -X PATCH -H "Content-Type: application/json" -d '{"workers": 1}' "$api/$id" >/dev/null
for _ in $(seq 60); do
    n=$(req "$api/$id" | python3 -c 'import json,sys; print(json.load(sys.stdin)["workers"])')
    [ "$n" -ge 1 ] && break
    sleep 3
done
echo "workers: $n"
[ "$n" -ge 1 ]
req -X DELETE "$api/$id"
SH
check "Dask extension: New cluster + scale to 1 worker on Slurm (labextension API)" bash /tmp/labext_check.sh

# JupyterLab must also come back after a restart once kernels have run (~/.ipython exists),
# e.g. after a workspace reboot
as_user 'mkdir -p ~/.ipython'
systemctl restart rsdat-jupyterlab
check "JupyterLab comes back after a restart" wait_http http://localhost/jupyter/api/status

if [ "$fail" = 0 ]; then echo "ALL CHECKS PASSED"; else echo "SOME CHECKS FAILED"; fi
exit $fail

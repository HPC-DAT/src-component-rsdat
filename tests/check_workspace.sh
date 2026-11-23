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
cat > /tmp/prefect_check.py <<'PY'
from prefect import flow

@flow
def hello():
    return 42

assert hello() == 42
print("flow ran against", __import__("prefect").settings.PREFECT_API_URL.value())
PY
chmod 644 /tmp/prefect_check.py
check "Prefect flow runs against the server (JupyterLab environment)" as_user "PREFECT_API_URL=http://127.0.0.1:4200/api $VENV/bin/python /tmp/prefect_check.py"
check "Slurm commands on PATH via the proxy" as_user 'test "$(command -v sbatch)" = /opt/rsdat/slurm-bin/sbatch && squeue >/dev/null'
cat > /tmp/sbatch_check.sh <<'SH'
set -e
printf '#!/bin/bash\n#SBATCH -J rsdat-check\n#SBATCH -t 00:05:00\nsleep 60\n' > /tmp/rsdat-check-job.sh
id=$(sbatch --parsable -D /tmp /tmp/rsdat-check-job.sh)
echo "job $id"
squeue | grep -q "$id"
scancel "$id"
SH
chmod 644 /tmp/sbatch_check.sh
check "sbatch/squeue/scancel through the REST API" as_user "bash /tmp/sbatch_check.sh"

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

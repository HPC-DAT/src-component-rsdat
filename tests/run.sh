#!/bin/bash
# End-to-end test: a workspace container (Ubuntu + systemd) gets the mock SRC-Nginx and
# rsdat.yml; Dask workers run on a single-node Slurm test cluster (SLURM-CLI-API-Proxy-client's
# testing/slurm-container image) and are submitted through the Slurm REST API.
#
#   tests/run.sh                 # build images if needed, run playbook, run checks
#   KEEP=1 tests/run.sh          # leave the containers running afterwards
#
# Env:
#   SLURM_IMAGE     Slurm test image (default slurm-cli-proxy-test; build it with
#                   testing/slurm-container in the SLURM-CLI-API-Proxy-client repo). The test
#                   adds an SSH client, iptables and bokeh to it (tests/Dockerfile.slurm).
#   SCHEDULER       where the Dask scheduler runs: 'workspace' (default) or 'cluster'
#   ROUTE           scheduler on the workspace: how it reaches the workers, 'gateway' (default)
#                   or 'direct'. With 'gateway' or SCHEDULER=cluster the workspace can't connect
#                   to the Slurm container except to slurmrestd, as on Snellius.
#   NETWORK_MODE    'bridge' (default: own docker network, works with Docker Desktop) or
#                   'host' (workspace on the host network, for hosts that need a proxy)
#   EXTRA_ENV       extra -e options for the ansible run (e.g. proxy settings)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
SLURM_IMAGE="${SLURM_IMAGE:-slurm-cli-proxy-test}"
WS_IMAGE="${WS_IMAGE:-rsdat-test-workspace}"
NETWORK_MODE="${NETWORK_MODE:-bridge}"
SCHEDULER="${SCHEDULER:-workspace}"
ROUTE="${ROUTE:-gateway}"
BLOCKED=$([ "$SCHEDULER" = cluster ] || [ "$ROUTE" = gateway ] && echo 1 || echo 0)
NET=rsdat-test
WS=rsdat-ws
SLURM=rsdat-slurm

cleanup() { if [ "${KEEP:-0}" != 1 ]; then docker rm -f "$WS" "$SLURM" >/dev/null 2>&1 || true; docker network rm "$NET" >/dev/null 2>&1 || true; fi; }
trap cleanup EXIT

docker image inspect "$SLURM_IMAGE" >/dev/null 2>&1 || { echo "Slurm test image $SLURM_IMAGE not found (see header)"; exit 1; }
# rebuilt when the worker requirements or the Dockerfile change
SLURM_SUM="$(cat "$REPO/worker-image/requirements.txt" "$HERE/Dockerfile.slurm" | sha256sum | cut -c1-12)"
SLURM_TEST_IMAGE="${SLURM_TEST_IMAGE:-$SLURM_IMAGE-rsdat:$SLURM_SUM}"
if ! docker image inspect "$SLURM_TEST_IMAGE" >/dev/null 2>&1; then
    ctx="$(mktemp -d)"
    cp "$HERE/Dockerfile.slurm" "$REPO/worker-image/requirements.txt" "$ctx/"
    docker build ${BUILD_OPTS:-} -t "$SLURM_TEST_IMAGE" --build-arg BASE="$SLURM_IMAGE" -f "$ctx/Dockerfile.slurm" "$ctx"
    rm -rf "$ctx"
fi
docker image inspect "$WS_IMAGE" >/dev/null 2>&1 || docker build ${BUILD_OPTS:-} -t "$WS_IMAGE" -f "$HERE/Dockerfile.workspace" "$HERE"

docker rm -f "$WS" "$SLURM" >/dev/null 2>&1 || true
docker network create "$NET" >/dev/null 2>&1 || true
docker run -d --name "$SLURM" --hostname localhost --network "$NET" -p 6822:6820 --cap-add NET_ADMIN "$SLURM_TEST_IMAGE" >/dev/null

SYSTEMD_OPTS=(--privileged --cgroupns=host -v /sys/fs/cgroup:/sys/fs/cgroup:rw --tmpfs /run --tmpfs /run/lock)
if [ "$NETWORK_MODE" = host ]; then
    docker run -d --name "$WS" --network host "${SYSTEMD_OPTS[@]}" "$WS_IMAGE" >/dev/null
    SLURM_URL="http://localhost:6822"
    # workers in the Slurm container reach the scheduler on the host via the docker network gateway
    CONTACT_HOST="$(docker network inspect -f '{{(index .IPAM.Config 0).Gateway}}' "$NET")"
else
    docker run -d --name "$WS" --hostname "$WS" --network "$NET" "${SYSTEMD_OPTS[@]}" "$WS_IMAGE" >/dev/null
    SLURM_URL="http://$SLURM:6820"
    CONTACT_HOST="$WS"
fi

# the Dask workers (in the Slurm container) must listen on this network
WORKER_NET="$(docker network inspect -f '{{(index .IPAM.Config 0).Subnet}}' "$NET")"
if [ "$BLOCKED" = 1 ]; then
    # like Snellius: the workspace may only reach slurmrestd; everything else must be opened
    # from the cluster side (the scheduler's SSH tunnel)
    if [ "$NETWORK_MODE" = host ]; then
        WS_IP="$CONTACT_HOST"     # the workspace's connections come from the docker gateway
    else
        WS_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$WS")"
    fi
    docker exec "$SLURM" iptables -A INPUT -p tcp --syn -s "$WS_IP" ! --dport 6820 -j REJECT --reject-with tcp-reset
    echo "Blocked connections from the workspace to the Slurm container (except port 6820)"
fi

echo -n "Waiting for Slurm"
for _ in $(seq 60); do docker exec "$SLURM" scontrol ping 2>/dev/null | grep -q UP && break; echo -n .; sleep 2; done; echo
TOKEN="$(docker exec "$SLURM" scontrol token username=testuser lifespan=7200 | cut -d= -f2)"

echo "Copying component into the workspace..."
docker exec "$WS" mkdir -p /opt/src-component-rsdat
tar -C "$REPO" --exclude=.git -cf - . | docker exec -i "$WS" tar -C /opt/src-component-rsdat -xf -

run_playbook() {
    # shellcheck disable=SC2086
    # with SRC's strict umask: everything the services and users need must get explicit modes
    docker exec ${EXTRA_ENV:-} -w /opt/src-component-rsdat "$WS" \
        sh -c 'umask 077; exec ansible-playbook "$@"' sh "$@"
}
# As on SRC workspaces: root's "python3" is a uv-managed Python in /root/.local (which rsdat
# can't use); the environment must still be made from Ubuntu's /usr/bin/python3
echo "== root's python3: a uv-managed Python in /root/.local"
docker exec "$WS" bash -c 'set -e; [ -x /usr/local/bin/python3 ] && exit 0
    /usr/bin/python3 -m venv /tmp/uv-bootstrap && /tmp/uv-bootstrap/bin/pip install -q uv
    UV_SYSTEM_CERTS=1 /tmp/uv-bootstrap/bin/uv python install -q 3.12
    ln -s "$(ls -d /root/.local/share/uv/python/cpython-3.12*/bin/python3.12 | head -1)" /usr/local/bin/python3
    echo "python3 is now $(readlink -f "$(command -v python3)")"'
echo "== mock SRC-Nginx"
run_playbook tests/mock_src_nginx.yml
echo "== rsdat.yml"
cat > /tmp/rsdat-test-vars.$$.yml <<VARS
rsdat_fqdn: localhost
rsdat_nginx_auth: sram
rsdat_slurm_api_url: "$SLURM_URL"
rsdat_slurm_jwt: "$TOKEN"
rsdat_dask_worker_python: /opt/venv/bin/python
rsdat_dask_queue: debug
rsdat_dask_memory: 500MiB
rsdat_dask_walltime: "00:15:00"
rsdat_dask_job_directory: /tmp
rsdat_dask_worker_network: "$WORKER_NET"
rsdat_dask_scheduler_location: $SCHEDULER
rsdat_dask_worker_route: $ROUTE
rsdat_dask_gateway_cores: 1
rsdat_dask_gateway_worker_cores: 1
rsdat_dask_gateway_worker_memory: 800MiB
rsdat_dask_gateway_memory: 1GiB
rsdat_dask_gateway_walltime: "00:15:00"
rsdat_dask_gateway_idle_timeout: 30
rsdat_dask_scheduler_memory: 500MiB
rsdat_dask_scheduler_walltime: "00:15:00"
rsdat_dask_tunnel_host: "$CONTACT_HOST"
rsdat_dask_contact_host: "$CONTACT_HOST"
# the Dask workers (Prefect tasks) reach the Prefect API through the mock nginx (no TLS)
rsdat_prefect_api_url: "http://$CONTACT_HOST/prefect/machine-api"
VARS
docker cp /tmp/rsdat-test-vars.$$.yml "$WS":/opt/src-component-rsdat/tests/test-vars.yml
rm -f /tmp/rsdat-test-vars.$$.yml
run_playbook rsdat.yml -e @tests/test-vars.yml
echo "== idempotence (second run)"
run_playbook rsdat.yml -e @tests/test-vars.yml | tee /tmp/rsdat-second-run.log | tail -3
grep -q "changed=0" /tmp/rsdat-second-run.log && echo "PASS idempotent" || { echo "FAIL second run changed something"; exit 1; }

echo "== checks"
docker exec -e RSDAT_EXPECT_WORKER_NET="$WORKER_NET" -e RSDAT_EXPECT_CONTACT_HOST="$CONTACT_HOST" \
    -e RSDAT_EXPECT_SCHEDULER="$SCHEDULER" -e RSDAT_EXPECT_ROUTE="$ROUTE" \
    -e RSDAT_GATEWAY_IDLE=30 \
    "$WS" bash /opt/src-component-rsdat/tests/check_workspace.sh
if docker exec "$SLURM" sh -c 'grep -l "Start worker at" /home/testuser/.rsdat/dask-logs/*.log' >/dev/null 2>&1; then
    echo "PASS worker job logs in ~/.rsdat/dask-logs on the cluster (created by the job)"
else
    echo "FAIL no worker job logs in ~/.rsdat/dask-logs on the cluster"; exit 1
fi
if [ "$SCHEDULER" = cluster ]; then
    if docker exec "$SLURM" sh -c 'grep -l "rsdat: scheduler at" /home/testuser/.rsdat/dask-logs/dask-scheduler-*.log' >/dev/null 2>&1; then
        echo "PASS scheduler job log on the cluster"
    else
        echo "FAIL no scheduler job log on the cluster"; exit 1
    fi
elif [ "$ROUTE" = gateway ]; then
    if docker exec "$SLURM" sh -c 'grep -l "rsdat: gateway into" /home/testuser/.rsdat/dask-logs/dask-gateway-*.log' >/dev/null 2>&1; then
        echo "PASS gateway job log on the cluster"
    else
        echo "FAIL no gateway job log on the cluster"; exit 1
    fi
fi
if [ "$BLOCKED" = 1 ]; then
    # the tunnel key may only open listening ports on the workspace's 127.0.0.1
    docker exec "$WS" cat /home/rsdat/.config/rsdat/dask-tunnel/id_ed25519 | docker exec -i "$SLURM" sh -c 'umask 077; cat > /tmp/tunnel-key'
    if docker exec "$SLURM" bash -c '
        o="-i /tmp/tunnel-key -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=5"
        ws='"$CONTACT_HOST"'
        out=$(timeout 15 ssh $o rsdat@$ws echo shell-works 2>/dev/null); [ "$out" != shell-works ] || { echo "shell allowed"; exit 1; }
        timeout 10 ssh $o -N -o ExitOnForwardFailure=yes -R 0.0.0.0:45678:127.0.0.1:22 rsdat@$ws 2>/dev/null && { echo "-R on 0.0.0.0 allowed"; exit 1; }
        timeout 10 ssh $o -N -L 45677:127.0.0.1:8888 rsdat@$ws 2>/dev/null & sleep 3
        if /opt/venv/bin/python -c "import urllib.request; urllib.request.urlopen(\"http://127.0.0.1:45677/jupyter/api/status\", timeout=5)" 2>/dev/null; then echo "-L allowed"; exit 1; fi
        exit 0'; then
        echo "PASS tunnel key: no shell, no -L, -R only on 127.0.0.1"
    else
        echo "FAIL tunnel key allows more than -R on 127.0.0.1"; exit 1
    fi
    docker exec "$SLURM" rm -f /tmp/tunnel-key
    if docker exec "$SLURM" sh -c 'ls -d ${TMPDIR:-/tmp}/rsdat-tls.* 2>/dev/null' | grep -q .; then
        echo "FAIL certificates/tunnel key left on the node"; exit 1
    else
        echo "PASS no certificates or tunnel key left on the node"
    fi
fi

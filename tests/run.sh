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
#   SCHEDULER       where the Dask scheduler runs: 'cluster' (default; the workspace can't
#                   connect to the Slurm container except to slurmrestd, as on Snellius) or
#                   'workspace' (connections in both directions)
#   NETWORK_MODE    'bridge' (default: own docker network, works with Docker Desktop) or
#                   'host' (workspace on the host network, for hosts that need a proxy)
#   EXTRA_ENV       extra -e options for the ansible run (e.g. proxy settings)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
SLURM_IMAGE="${SLURM_IMAGE:-slurm-cli-proxy-test}"
WS_IMAGE="${WS_IMAGE:-rsdat-test-workspace}"
NETWORK_MODE="${NETWORK_MODE:-bridge}"
SCHEDULER="${SCHEDULER:-cluster}"
NET=rsdat-test
WS=rsdat-ws
SLURM=rsdat-slurm

cleanup() { if [ "${KEEP:-0}" != 1 ]; then docker rm -f "$WS" "$SLURM" >/dev/null 2>&1 || true; docker network rm "$NET" >/dev/null 2>&1 || true; fi; }
trap cleanup EXIT

docker image inspect "$SLURM_IMAGE" >/dev/null 2>&1 || { echo "Slurm test image $SLURM_IMAGE not found (see header)"; exit 1; }
SLURM_TEST_IMAGE="${SLURM_TEST_IMAGE:-$SLURM_IMAGE-rsdat}"
BOKEH="$(sed -nE 's/^bokeh==(.+)/\1/p' "$REPO/worker-image/requirements.txt")"
docker image inspect "$SLURM_TEST_IMAGE" >/dev/null 2>&1 || docker build ${BUILD_OPTS:-} -t "$SLURM_TEST_IMAGE" \
    --build-arg BASE="$SLURM_IMAGE" --build-arg BOKEH="$BOKEH" -f "$HERE/Dockerfile.slurm" "$HERE"
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
if [ "$SCHEDULER" = cluster ]; then
    # like Snellius: the workspace may only reach slurmrestd; everything else must be opened
    # from the cluster side (the scheduler's SSH tunnel)
    if [ "$NETWORK_MODE" = host ]; then
        WS_IP="$CONTACT_HOST"     # the workspace's connections come from the docker gateway
    else
        WS_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$WS")"
    fi
    docker exec "$SLURM" iptables -A INPUT -p tcp --syn -s "$WS_IP" ! --dport 6820 -j REJECT --reject-with tcp-reset
    echo "Blocked connections from the workspace to the Slurm container (except slurmrestd)"
fi

echo -n "Waiting for Slurm"
for _ in $(seq 60); do docker exec "$SLURM" scontrol ping 2>/dev/null | grep -q UP && break; echo -n .; sleep 2; done; echo
TOKEN="$(docker exec "$SLURM" scontrol token username=testuser lifespan=7200 | cut -d= -f2)"

echo "Copying component into the workspace..."
docker exec "$WS" mkdir -p /opt/src-component-rsdat
tar -C "$REPO" --exclude=.git -cf - . | docker exec -i "$WS" tar -C /opt/src-component-rsdat -xf -

run_playbook() {
    # shellcheck disable=SC2086
    docker exec ${EXTRA_ENV:-} -w /opt/src-component-rsdat "$WS" ansible-playbook "$@"
}
echo "== mock SRC-Nginx"
run_playbook tests/mock_src_nginx.yml
echo "== rsdat.yml"
cat > /tmp/rsdat-test-vars.$$.yml <<VARS
rsdat_fqdn: localhost
rsdat_nginx_auth: sram
rsdat_slurm_api_url: "$SLURM_URL"
rsdat_slurm_user: testuser
rsdat_slurm_jwt: "$TOKEN"
rsdat_dask_worker_python: /opt/venv/bin/python
rsdat_dask_queue: debug
rsdat_dask_memory: 500MiB
rsdat_dask_walltime: "00:15:00"
rsdat_dask_job_directory: /tmp
rsdat_dask_worker_network: "$WORKER_NET"
rsdat_dask_scheduler_location: $SCHEDULER
rsdat_dask_scheduler_memory: 500MiB
rsdat_dask_scheduler_walltime: "00:15:00"
rsdat_dask_tunnel_host: "$CONTACT_HOST"
rsdat_dask_contact_host: "$CONTACT_HOST"
VARS
docker cp /tmp/rsdat-test-vars.$$.yml "$WS":/opt/src-component-rsdat/tests/test-vars.yml
rm -f /tmp/rsdat-test-vars.$$.yml
run_playbook rsdat.yml -e @tests/test-vars.yml
echo "== idempotence (second run)"
run_playbook rsdat.yml -e @tests/test-vars.yml | tee /tmp/rsdat-second-run.log | tail -3
grep -q "changed=0" /tmp/rsdat-second-run.log && echo "PASS idempotent" || { echo "FAIL second run changed something"; exit 1; }

echo "== checks"
docker exec -e RSDAT_EXPECT_WORKER_NET="$WORKER_NET" -e RSDAT_EXPECT_CONTACT_HOST="$CONTACT_HOST" \
    -e RSDAT_EXPECT_SCHEDULER="$SCHEDULER" \
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
    if docker exec "$SLURM" sh -c 'ls -d ${TMPDIR:-/tmp}/rsdat-tls.* 2>/dev/null' | grep -q .; then
        echo "FAIL certificates/tunnel key left on the node"; exit 1
    else
        echo "PASS no certificates or tunnel key left on the node"
    fi
fi

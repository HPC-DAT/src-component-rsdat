# The Slurm test image plus what a Snellius compute node has and the test needs: an SSH client
# (the gateway's and scheduler's tunnels), iptables (blocking the workspace like the cluster
# firewall), and the worker image's environment: the same OS Python and packages
# (worker-image/python-version and requirements.lock, copied into the build context by run.sh)
# in /opt/venv, installed as worker-image/worker.def does.
ARG BASE=slurm-cli-proxy-test
FROM ${BASE}
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends openssh-client iptables \
    && rm -rf /var/lib/apt/lists/*
COPY python-version requirements.lock /opt/rsdat/
RUN test "$(python3 -c 'import sys; print(sys.version.split()[0])')" = "$(cat /opt/rsdat/python-version)" \
    && /opt/venv/bin/pip install --no-cache-dir uv==0.11.32 && cp /opt/venv/bin/uv /usr/local/bin/uv \
    && rm -rf /opt/venv \
    && export UV_NO_CACHE=1 UV_PYTHON_DOWNLOADS=never UV_SYSTEM_CERTS=1 \
    && uv venv --python /usr/bin/python3 /opt/venv \
    && uv pip sync --python /opt/venv/bin/python /opt/rsdat/requirements.lock
# Scheduler job + two worker jobs need 3 CPUs; CI machines often have 2. Declare at least 4 (the
# test jobs are light) and let slurmd accept a node definition above the hardware.
RUN sed -i '/^MEM=/i NODE_LINE=$(echo "$NODE_LINE" | awk '"'"'{ for (i = 1; i <= NF; i++) { if ($i ~ /^CPUs=/ && substr($i, 6) + 0 < 4) $i = "CPUs=4"; if ($i ~ /^CoresPerSocket=/) c = i; if ($i ~ /^SocketsPerBoard=/) s = i; if ($i ~ /^ThreadsPerCore=/) t = i } if (s) $s = "SocketsPerBoard=1"; if (t) $t = "ThreadsPerCore=1"; split($0, a, "CPUs="); split(a[2], b, " "); if (c) $c = "CoresPerSocket=" b[1]; print }'"'"')' /usr/local/bin/entrypoint.sh \
    && grep -q '^SlurmdParameters' /etc/slurm/slurm.conf || echo 'SlurmdParameters=config_overrides' >> /etc/slurm/slurm.conf

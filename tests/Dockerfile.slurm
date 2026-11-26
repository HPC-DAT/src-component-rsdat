# The Slurm test image plus what a Snellius compute node has and the test needs: an SSH client
# (the gateway's and scheduler's tunnels), an SSH server and python3 (the login node for Slurm
# over SSH and sshuttle; started by run.sh when used), iptables (blocking the workspace like the cluster
# firewall), and the worker image's packages (worker-image/requirements.txt, copied into the
# build context by run.sh) in the Dask environment.
ARG BASE=slurm-cli-proxy-test
FROM ${BASE}
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends openssh-client openssh-server python3 iptables \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/worker-requirements.txt
RUN /opt/venv/bin/pip install --no-cache-dir -r /tmp/worker-requirements.txt
# Scheduler job + two worker jobs need 3 CPUs; CI machines often have 2. Declare at least 4 (the
# test jobs are light) and let slurmd accept a node definition above the hardware.
RUN sed -i '/^MEM=/i NODE_LINE=$(echo "$NODE_LINE" | awk '"'"'{ for (i = 1; i <= NF; i++) { if ($i ~ /^CPUs=/ && substr($i, 6) + 0 < 4) $i = "CPUs=4"; if ($i ~ /^CoresPerSocket=/) c = i; if ($i ~ /^SocketsPerBoard=/) s = i; if ($i ~ /^ThreadsPerCore=/) t = i } if (s) $s = "SocketsPerBoard=1"; if (t) $t = "ThreadsPerCore=1"; split($0, a, "CPUs="); split(a[2], b, " "); if (c) $c = "CoresPerSocket=" b[1]; print }'"'"')' /usr/local/bin/entrypoint.sh \
    && grep -q '^SlurmdParameters' /etc/slurm/slurm.conf || echo 'SlurmdParameters=config_overrides' >> /etc/slurm/slurm.conf

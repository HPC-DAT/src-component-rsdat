# The Slurm test image plus what a Snellius compute node has and the test needs: an SSH client
# (the scheduler job's tunnel), iptables (blocking the workspace like the cluster firewall), and
# bokeh in the Dask environment (dashboard).
ARG BASE=slurm-cli-proxy-test
FROM ${BASE}
ARG BOKEH
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends openssh-client iptables \
    && rm -rf /var/lib/apt/lists/*
RUN /opt/venv/bin/pip install --no-cache-dir "bokeh==${BOKEH}"
# Scheduler job + two worker jobs need 3 CPUs; CI machines often have 2. Declare at least 4 (the
# test jobs are light) and let slurmd accept a node definition above the hardware.
RUN sed -i '/^MEM=/i NODE_LINE=$(echo "$NODE_LINE" | awk '"'"'{ for (i = 1; i <= NF; i++) { if ($i ~ /^CPUs=/ && substr($i, 6) + 0 < 4) $i = "CPUs=4"; if ($i ~ /^CoresPerSocket=/) c = i; if ($i ~ /^SocketsPerBoard=/) s = i; if ($i ~ /^ThreadsPerCore=/) t = i } if (s) $s = "SocketsPerBoard=1"; if (t) $t = "ThreadsPerCore=1"; split($0, a, "CPUs="); split(a[2], b, " "); if (c) $c = "CoresPerSocket=" b[1]; print }'"'"')' /usr/local/bin/entrypoint.sh \
    && grep -q '^SlurmdParameters' /etc/slurm/slurm.conf || echo 'SlurmdParameters=config_overrides' >> /etc/slurm/slurm.conf

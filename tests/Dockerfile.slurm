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

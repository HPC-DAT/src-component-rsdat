#!/bin/bash
# Resolve the environments once, with exact versions of every package (also indirect ones):
#   workspace.lock     <- workspace.in (JupyterLab, scheduler, Prefect server, + requirements.txt)
#   requirements.lock  <- requirements.txt, constrained to workspace.lock: the workers get the
#                         same version of every package they share with the workspace
# The playbook installs workspace.lock, the worker image requirements.lock, both on Python
# $(cat python-version). Run after changing workspace.in, requirements.txt or python-version;
# the workflow fails when the locks are out of date.
#
#   worker-image/lock.sh            # keep the versions that still fit, add what's new
#   worker-image/lock.sh --upgrade  # newest versions that fit the pins
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
py=$(cat python-version)
opts=(--quiet --python-version "$py" --python-platform x86_64-manylinux_2_28 --no-header
      --generate-hashes)
uv pip compile "${opts[@]}" "$@" workspace.in -o workspace.lock
uv pip compile "${opts[@]}" "$@" requirements.txt -c workspace.lock -o requirements.lock
# every package the workers have is in the workspace with the same version
python3 - <<'PY'
import re
def pins(f):
    return dict(re.findall(r"^([A-Za-z0-9_.-]+)==([^ \\]+)", open(f).read(), re.M))
ws, wk = pins("workspace.lock"), pins("requirements.lock")
bad = {n: (v, ws.get(n)) for n, v in wk.items() if ws.get(n) != v}
assert not bad, f"worker and workspace versions differ: {bad}"
print(f"workspace.lock: {len(ws)} packages, requirements.lock: {len(wk)} (all in the workspace, same versions)")
PY

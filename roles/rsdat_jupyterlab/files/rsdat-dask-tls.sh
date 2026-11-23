#!/bin/bash
# Create a private CA and one certificate/key pair for the Dask scheduler, workers and
# clients of this workspace (installed by src-component-rsdat).
#   rsdat-dask-tls.sh <directory> [days]
# Existing files are kept; delete the directory to create new ones (then restart JupyterLab).
set -euo pipefail
dir="$1"
days="${2:-3650}"
[ -s "$dir/dask.pem" ] && [ -s "$dir/dask.key" ] && [ -s "$dir/ca.pem" ] && exit 0
umask 077
mkdir -p "$dir"
cd "$dir"
openssl req -x509 -newkey rsa:3072 -nodes -days "$days" -subj "/CN=rsdat-dask-ca" \
    -keyout ca.key -out ca.pem \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
openssl req -newkey rsa:3072 -nodes -subj "/CN=rsdat-dask" -keyout dask.key -out dask.csr 2>/dev/null
printf 'basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:rsdat-dask\n' > ext.cnf
openssl x509 -req -in dask.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days "$days" \
    -extfile ext.cnf -out dask.pem 2>/dev/null
# the CA key is not needed afterwards: nobody can issue further certificates
rm -f ca.key ca.srl dask.csr ext.cnf
chmod 600 ca.pem dask.pem dask.key
echo "created $dir/{ca.pem,dask.pem,dask.key}"

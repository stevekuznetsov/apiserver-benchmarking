#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

config="${1:-}"
if [[ ! -s "${config}" ]]; then
  echo "Usage: $0 [config-file]"
  exit 1
fi
config="$( realpath "${config}" )"
workdir="$( jq '.setup.output_dir' --raw-output <"${config}" )"
tempdir="$( mktemp -d )"
cp "${config}" "${tempdir}/config.json"

function cleanup() {
  echo "Terminating child processes..."
  for child in $( jobs -p ); do
    kill -SIGTERM "${child}" || true
  done
  wait

  mv "${tempdir}/config.json" "${workdir}"
  rm -rf "${tempdir}"
  chmod a+rw --recursive "${workdir}"
  chown stevekuznetsov:stevekuznetsov --recursive "${workdir}"
  echo "Done!"
}
trap cleanup EXIT

echo "Compiling utilities..."
pushd /home/stevekuznetsov/code/stevekuznetsov/apiserver-benchmarking/src/github.com/stevekuznetsov/apiserver-benchmarking
GOPATH=/home/stevekuznetsov/code/stevekuznetsov/apiserver-benchmarking/ make build
bindir="$( realpath ./bin )"
popd

echo "Building API server..."
if [[ -n "${REBUILD_APISERVER:-}" ]]; then
  pushd /home/stevekuznetsov/code/kcp-dev/kubernetes/src/github.com/kcp-dev/kubernetes
#  GOPATH=/home/stevekuznetsov/code/kcp-dev/kubernetes/ make quick-release-images
  image_tar="$( realpath _output/release-images/amd64/kube-apiserver.tar )"
  jq --arg path "${image_tar}" '.setup.reload_api_server_image_from=$path' "${config}" > "${tempdir}/config.json"
  popd
fi

echo "Starting cAdvisor..."
# we need to run cAdvisor + podman, which is not yet supported ...
# so, we need to compile it ourselves and what-not
# git clone git@github.com:google/cadvisor.git
# cd cadvisor
# git pull origin/pull/3021/merge:podman
# git checkout podman
# local hacks ...
# make build
# chmod +x cadvisor
# --- (as root)
# dnf install -y podman podman-docker
# systemctl start podman.service
# cadvisor &
pushd /home/stevekuznetsov/code/google/cadvisor/src/github.com/google/cadvisor
if [[ -n "${REBUILD_CADVISOR:-}" ]]; then
  GOPATH=/home/stevekuznetsov/code/google/cadvisor/ make build
fi
./cadvisor &
popd

echo "Running benchmark..."
"${bindir}/runner" --config "${tempdir}/config.json"

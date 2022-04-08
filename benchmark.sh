#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

config="${1:-}"
if [[ ! -s "${config}" ]]; then
  echo "Usage: $0 [config-file]"
  exit 1
fi

workdir="${WORKDIR:-"$( mktemp -d )"}"
rm -rf "${workdir}"
mkdir -p "${workdir}"
function cleanup() {
  echo "Terminating child processes..."
  for child in $( jobs -p ); do
    kill -SIGTERM "${child}" || true
  done
  wait
  echo "Capturing logs..."
  for container in $( podman pod ps --format json | jq '.[] | select(.Name == "benchmark") | .Containers[].Id' --raw-output ); do
    name="$( podman ps -a --format json | jq ".[] | select(.Id==\"${container}\") | .Names[0]" --raw-output )"
    podman logs "${container}" > "${workdir}/${name}.log"
    podman stop "${container}"
  done
  echo "Cleaning up..."
  podman pod rm --force benchmark

  echo "Digesting pprof data..."
  for program in etcd apiserver; do
    for profile in heap profile; do
      profile_dir="${workdir}/profiles/${program}/${profile}"
      ./bin/pprof-digest --input "${profile_dir}" --output "${profile_dir}/pprof.json"
    done
  done
  sudo chmod a+rw --recursive "${workdir}"
  sudo chown stevekuznetsov:stevekuznetsov --recursive "${workdir}"
  echo "Done!"
}
trap cleanup EXIT

echo "Generating keys..."
mkdir -p "${workdir}/keys"
openssl genrsa -out "${workdir}/keys/service-account-key.pem" 4096
openssl req -new -x509 -days 365 -key "${workdir}/keys/service-account-key.pem" -sha256 -out "${workdir}/keys/service-account.pem" -subj '/C=US/ST=Washington/L=Seattle/O=Kubernetes Dev Inc/OU=Storage/CN=crdb.storage.k8s.io'

echo "Compiling utilities..."
GOPATH=/home/stevekuznetsov/code/stevekuznetsov/apiserver-benchmarking/ make build
bindir="$( realpath ./bin )"

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

mkdir -p "${workdir}/tokens"
cat >"${workdir}/tokens/auth-tokens.csv" <<EOF
admin-token,admin,f4bf6eb5-aa2f-45db-acde-1c217748b22c,"system:masters"
EOF

function resolve_apiserver_image() {
  podman images --format json | jq '.[] | select(.Names != null) | select(.Names[0] | startswith("k8s.gcr.io/kube-apiserver-amd64")) | .Names[0]' --raw-output
}

if [[ -n "${RELOAD_IMAGE:-}" ]]; then
  echo "Reloading apiserver image..."
  apiserver_image="$( resolve_apiserver_image )"
  if [[ -n "${apiserver_image:-}" ]]; then
    podman rmi "${apiserver_image}"
  fi
  podman load --input /home/stevekuznetsov/code/kcp-dev/kubernetes/src/github.com/kcp-dev/kubernetes/_output/release-images/amd64/kube-apiserver.tar
fi
apiserver_image="$( resolve_apiserver_image )"
echo "Running with apiserver image ${apiserver_image}."

echo "Starting pod..."
podman pod create --name benchmark --publish 8080
if [[ "$( jq '.setup?.storage_backend?' --raw-output <"${config}" )" == "crdb" ]]; then
  podman run -dt --pod benchmark --name etcd docker.io/cockroachdb/cockroach:v21.2.8 start-single-node --sql-addr localhost:2379 --advertise-sql-addr localhost:2379 --insecure
else
  podman run -dt --pod benchmark --name etcd quay.io/coreos/etcd:v3.5.1 etcd --enable-pprof --quota-backend-bytes 6710886400
fi
podman run -dt --pod benchmark --name apiserver \
  --volume "${workdir}/tokens:/tokens:z" \
  --volume "${workdir}/keys:/var/lib/kubernetes:z" \
  "${apiserver_image}" \
  kube-apiserver \
  --service-account-key-file=/var/lib/kubernetes/service-account.pem \
  --service-account-signing-key-file=/var/lib/kubernetes/service-account-key.pem \
  --service-account-issuer=kubernetes.default.svc.cluster.local \
  --token-auth-file=/tokens/auth-tokens.csv $( "${bindir}/runner" --config "${config}" --print-apiserver-flags )

echo "Launching monitoring..."
mkdir -p "${workdir}/profiles"
for profile in heap profile; do
  query=""
  if [[ "${profile}" == "profile" ]]; then
    query="--query seconds=30 --interval 30s"
  fi
  podman run -dt --pod benchmark --name "apiserver-${profile}-gather" --volume "${bindir}:/benchmark:z" --volume "${workdir}/profiles:/profiles:z" quay.io/fedora/fedora:35 /benchmark/pprof-fetch --pprof-url https://0.0.0.0:6443 --metric "${profile}" --output /profiles/apiserver --header "Authorization=Bearer admin-token" ${query}
  podman run -dt --pod benchmark --name "etcd-${profile}-gather" --volume "${bindir}:/benchmark:z" --volume "${workdir}/profiles:/profiles:z" quay.io/fedora/fedora:35 /benchmark/pprof-fetch  --pprof-url http://0.0.0.0:2379 --metric "${profile}" --output /profiles/etcd ${query}
done

mkdir "${workdir}/profiles/cadvisor"
for container in etcd apiserver; do
  basepath="${workdir}/profiles/cadvisor/${container}"
  "${bindir}/cadvisor" --cadvisor-url "http://localhost:8080" --container "${container}" --output "${basepath}.json" >"${basepath}.log" 2>&1 &
done

echo "Saving profiles to ${workdir}/profiles/ ..."

sleep 10
cp "${config}" "${workdir}/config.json"
podman run -dt --pod benchmark --name "runner" --volume "${bindir}:/benchmark:z" --volume "${workdir}:/workdir:z" quay.io/fedora/fedora:35 /benchmark/runner --config /workdir/config.json --apiserver-url "https://localhost:6443" --events "/workdir/events.json" --token admin-token
podman logs -f runner
sleep 10
# apiserver-benchmarking
Tools for benchmarking the performance of the Kubernetes API server with different storage backends.

## Setup
This repository uses the `podman` tool to run the Kubernetes API server and `cAdvisor` and `pprof` to record performance
data for them under load.

### `podman`

Install `podman` with:

```
$ dnf install -y podman-3:3.4.4
```

If using root-less containers, ensure you are using cGroups v2. In order to check, use:

```
$ podman info --format json | jq .host.cgroupVersion
"v2"
```

If you are not, ensure you are on a new-enough kernel and turn them on with:

```
$ dnf install -y grubby
$ grubby --update-kernel=ALL --args="systemd.unified_cgroup_hierarchy=1"
```

If your machine does not support this, use root-full containers by running all the benchmarking tools with `sudo`.

### `cAdvisor`

We require an experimental build of `cAdvisor` to support `podman` containers. Build it with:

```
$ git clone git@github.com:google/cadvisor.git
$ cd cadvisor
$ git pull origin/pull/3021/merge:podman
$ git checkout podman
$ make build
$ chmod +x cadvisor
```

In order to run `cAdvisor`, you will need to expose the `podman` socket for it:

```
$ systemctl --user enable podman
$ systemctl --user start podman
```

Then, simply run the built binary:

```
$ ./cadvisor &
```

### `kube-apiserver`

We require an experimental version of the Kubernetes API server to support CockroachDB as a backing store. Build it with:

```
$ git clone git@github.com:stevekuznetsov/kubernetes.git
$ cd kubernetes
$ git checkout skuznets/generic-storage
$ make quick-release-images
```

## Compiling Tools

From the root of this repository, run:

```
$ make build
```

Tools will be output to `./bin`

## Running Benchmarks

When `cAdvisor` is up, running a benchmark is as easy as:

```
./bin/runner --config ./config/throughput/crdb.json
```

On the first execution and in every case that the Kubernetes API server image is re-built, you will want to re-load the
image into the registry. From the `kubernetes` repository, find the path for the image tarball with:

```
$ image_tar="$( realpath _output/release-images/amd64/kube-apiserver.tar )"
```

Inject this into the configuration file you are using with `jq` or manually:

```
$ jq --arg path "${image_tar}" '.setup.reload_api_server_image_from=$path' "original-config.json" > "updated-config.json"
$ ./bin/runner --config ./updated-config.json
```

### Accessing The System During A Benchmark

By default, the Kubernetes API server and the database are exposed to the host, along with one of the CRDB consoles.

To make a request against the Kubernetes API, use:

```
$ kubectl --insecure-skip-tls-verify --server https://$( podman port apiserver 6443/tcp ) --token admin-token get ns
```

To access the CockroachDB SQL shell:

```
$ podman exec -it database0 cockroach sql --insecure
```

To access the CockroachDB console:

```
$ firefox http://$( podman port database0 8080/tcp )
```

## Generating Visualizations

In order to generate the visualizations, you will need Python 3 and the following libraries:

```
$ pip install numpy matplotlib seaborn pandas flexitext
```

The following will run all the benchmarks necessary and generate the visualizations:

```sh
for config in $( find config/ -mindepth 2 -type f ); do
  ./bin/runner --config "${config}"
done

./cmd/visualizer/comparative-client-metrics-ha.py /tmp/benchmark/throughput/* ./figures
./cmd/visualizer/index.py /tmp/benchmark/indexed/* ./figures
./cmd/visualizer/insert.py /tmp/benchmark/indexed/crdb /tmp/benchmark/throughput/crdb ./figures
./cmd/visualizer/watch.py /tmp/benchmark/watch/* ./figures
```

NOTE: Some benchmarks require enormous amounts of system resources, or large amounts of time, or both.
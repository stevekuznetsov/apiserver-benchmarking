package config

import (
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Config struct {
	// Setup configures the infrastructure on which we will be running.
	Setup *Setup `json:"setup,omitempty"`
	// Seed configures the pre-benchmark steps to seed the API server.
	Seed *Seed `json:"seed,omitempty"`
	// Interact configures the benchmark steps to interact with the API server.
	Interact *Interact `json:"interact,omitempty"`
}

type Setup struct {
	// ReloadAPIServerImageFrom is the path to a tarball from which the API server image should be loaded.
	ReloadAPIServerImageFrom string `json:"reload_api_server_image_from,omitempty"`
	// OutputDir is where we put logs, artifacts, profiling, etc. We will remove *all contents* of this
	// directory on startup, so ensure this is set to an appropriate value. Removing at the start of a run
	// enables us to keep artifacts around after we're done but also not pollute the filesystem with many
	// runs in sequence.
	OutputDir string `json:"output_dir,omitempty"`

	// StorageBackend is the storage backend to use, either etcd3 or crdb. Defaults to etcd3.
	StorageBackend string `json:"storage_backend,omitempty"`
	// SingleNodeStorageTopology determines if we run just one instance of the backend or multiple.
	SingleNodeStorageTopology bool `json:"single_node_storage_topology,omitempty"`
	// WatchCacheEnabled determines if the watch cache should be enabled. Defaults to false.
	WatchCacheEnabled bool `json:"watch_cache_enabled,omitempty"`
}

type Seed struct {
	// FillSize is the size (in bytes) of filler data to be placed in every object
	FillSize int `json:"fill_size"`
	// Count is the number of objects to seed the API server with.
	Count int `json:"count,omitempty"`
	// Parallelism determines how many parallel client connections to use
	Parallelism int `json:"parallelism,omitempty"`
}

type Interact struct {
	// Throughput determines the ratio of different requests to make when testing request throughput and latency
	Throughput *Proportions `json:"throughput,omitempty"`

	// Selectivity configures the different types of requests to make when testing indexed selectivity
	Selectivity *Selectivity `json:"selectivity,omitempty"`

	// Watch configures the tests for watch performance and throughput
	Watch *Watch `json:"watch,omitempty"`

	// Parallelism determines how many parallel client connections to use
	Parallelism int `json:"parallelism,omitempty"`
	// Operations is the total number of client operations to execute
	Operations int `json:"operations,omitempty"`
}

type Proportions struct {
	Create int `json:"create,omitempty"`
	Get    int `json:"get,omitempty"`
	List   int `json:"list,omitempty"`
	Update int `json:"update,omitempty"`
	Delete int `json:"delete,omitempty"`
}

type Selectivity struct {
	*Seed `json:",inline"`

	// Minimum is the size of the most selective set we will query for.
	Minimum int `json:"minimum,omitempty"`
}

type Watch struct {
	*Seed `json:",inline"`

	// Partitions determines how we break up the objects we're creating.
	Partitions int `json:"partitions,omitempty"`
	// WatchersPerPartition determines the denisty of watchers
	WatchersPerPartition int `json:"watchers_per_partition,omitempty"`

	// Duration is how long we want to run this test
	Duration v1.Duration `json:"duration,omitempty"`
}
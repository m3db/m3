# Cluster Integration Tests

Cluster integration tests are purely Go tests that allow us to test behavior within a single M3 component or across multiple components in a setup that closely resembles an actual M3 deployment. 

## Overview

### Motivation & Use
Cluster integration tests were created to allow us to write tests that were faster to run, easier to write, and easier to debug than the previously docker-based counterparts.

These integration tests can be run via `go test` (and subsequently an IDE). They look similar to the standard unit test you'd write in Go (e.g. `func TestFoo(t *testing.T)`).

### API
The integration test framework provides an API for creating and interacting with a cluster. The API and its implementation lives [here](https://github.com/m3db/m3/tree/master/src/integration/resources). This [types file](https://github.com/m3db/m3/blob/11a38384efb6d00f26536941e8265009931ead06/src/integration/resources/types.go#L55-L208) outlines the objects representing M3 components and the API calls that can be made to each component. Let's review the cluster interface:

_M3 Interface_

```golang
// M3Resources represents a set of test M3 components.
type M3Resources interface {
	// Cleanup cleans up after each started component.
	Cleanup() error
	// Nodes returns all node resources.
	Nodes() Nodes
	// Coordinator returns the coordinator resource.
	Coordinator() Coordinator
	// Aggregators returns all aggregator resources.
	Aggregators() Aggregators
}
```
An instantiation of the interface above gives us an M3 cluster to operate on. DB nodes, aggregators, and the coordinator can be manipulated as desired by retrieving the component and invoking API calls. See the types file above to see all the operations that can be done on each component.


### When to Use
Consider writing a cluster integration test when any of the following apply:

* Test involves multiple parts of a component
* Test requires cross component communication
* Test can be driven entirely by component config and API calls
* Investigating an issue where you'd typically spin up a real cluster
* Local development

### When Not to Use
There are some occasions where cluster integration tests may not be the best tool. Consider other options if:

* Test requires you to manipulate the clock.
   * This is currently unsupported. Component-specific integration tests that are configured programmatically may be a better option. Here are some examples for [dbnodes](https://github.com/m3db/m3/tree/master/src/dbnode/integration)
* Test requires changing options not exposed by config.
    * Since cluster integration tests start M3 components via the same entry point as the actual binary, all configuration has to be done via the component configuration file or public APIs. If you need to manipulate some options that are not exposed this way, then these tests may not be a good fit.

## Example Usage

Let's walk through a few examples that outline the most common use cases.

### Denoting test as a cluster integration test
Any test meant to be considered as a cluster integration test should be tagged with the correct build tag:

```golang
// +build cluster_integration
```

### Spinning up a cluster


Below is an example that will spin up a cluster with a coordinator, DB node, and aggregator that can interact with each other. It also creates you an unaggregated and an aggregated namespace that you can read and write to.

```golang
import (
	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/integration/resources/inprocess"
)

// The {} represent empty configs which will start each component
// with the default configuration
cfgs, _ := inprocess.NewClusterConfigsFromYAML(
	`db: {}`, `{}`, `{}`,
)

m3, _ = inprocess.NewCluster(cfgs,
	resources.ClusterOptions{
		DBNode: resources.NewDBNodeClusterOptions(),
		Aggregator: resources.NewAggregatorClusterOptions()
	},
)
```

Naturally, you can spin the cluster up with whatever configuration you like. Also, you can use `DBNodeClusterOptions` and `AggregatorClusterOptions` to spin up more interesting clusters. For example:

```golang
m3, _ := inprocess.NewCluster(configs, resources.ClusterOptions{
	DBNode: &resources.DBNodeClusterOptions{
		RF:                 3,
		NumInstances:       1,
		NumShards:          4,
		NumIsolationGroups: 3,
	},
	Aggregator: &resources.AggregatorClusterOptions{
		RF:                 2,
		NumShards:          4,
		NumInstances:       2,
		NumIsolationGroups: 2,
	}
})
```
This configuration will spin up the following:

* DB nodes with an RF of 3 and 1 instance for each RF (i.e. 3 separate invocations of M3DB in a single process)
* 1 coordinator
* Aggregators with an RF of 2 and 2 instances for each RF (i.e. 4 separate aggregator invocations in a single process)

It's worth pointing out that you aren't required to spin up a full cluster each time. `inprocess.NewCluster` also allows you to just spin up a dbnode and coordinator, and later we demonstrate how to spin up just a single component.

### Reading and writing to a cluster
Continuing from where we left off in the [Spinning up a cluster](#Spinning up a cluster) section, let's read and write some data to the new cluster.

```golang
// Write some data
coordinator := m3.Coordinator()

/*
 * Using this method on the resources.Coordinator interface:
 *
 *	// WriteProm writes a prometheus metric. Takes tags/labels as a map for convenience.
 *  WriteProm(name string, tags map[string]string, samples []prompb.Sample, headers Headers) error
 *  
 */
_ := coordinator.WriteProm("foo_metric", map[string]string{
	"bar_label":            "baz",
}, []prompb.Sample{
	{
		Value:     42,
		Timestamp: storage.TimeToPromTimestamp(xtime.Now()),
	},
}, nil)

// Read some data
/*
 * Using this method on the resources.Coordinator interface:
 *
 *  // RangeQuery runs a range query with provided headers
 *  RangeQuery(req RangeQueryRequest, headers Headers) (model.Matrix, error)
 *  
 */
result, err := coord.RangeQuery(
	resources.RangeQueryRequest{
		Query: "foo_metric",
		Start: time.Now().Add(-30 * time.Second),
		End:   time.Now(),
	},
	nil)

```
NB: If using this code in tests, the `RangeQuery` may need to be retried to yield a result. Writes in M3 are async by default, so they're not immediately available for reads. `resources.Retry` is provided for convenience to assist with this.

### Spinning up an external resource
Occasionally, it is convenient to test M3 alongside some component it works closely with. Prometheus is the most common example. The cluster integration test framework supports this by spinning up external resources in docker containers. External resources must adhere to the following interface:

```golang
// ExternalResources represents an external (i.e. non-M3)
// resource that we'd like to be able to spin up for an
// integration test.
type ExternalResources interface {
	// Setup sets up the external resource so that it's ready
	// for use.
	Setup() error

	// Close stops and cleans up all the resources associated with
	// the external resource.
	Close() error
}
```

Since it's so commonly used in conjunction with M3, an implementation for [Prometheus](https://github.com/m3db/m3/blob/master/src/integration/resources/docker/prometheus.go) already exists.

Here's an example of a test spinning up M3 and Prometheus:

```golang
cfgs, _ := inprocess.NewClusterConfigsFromConfigFile(pathToDBCfg, pathToCoordCfg, "")
m3, _ := inprocess.NewCluster(cfgs,
        resources.ClusterOptions{
                DBNode: resources.NewDBNodeClusterOptions(),
        },
)
defer m3.Cleanup()

// Spin up external resources. In this case, prometheus.
pool, _ := dockertest.NewPool("")

prom := docker.NewPrometheus(docker.PrometheusOptions{
	Pool:      pool,
	PathToCfg: pathToPromCfg,
})
prom.Setup()
defer prom.Close()

// Run tests...


```

### Spinning up an individual component
We've been mostly referring to a cluster with a coordinator, db node, and potentially an aggregator, but it's also possible to spin up each component individually. Each component has the same handful of constructors in the `inprocess` [package](https://github.com/m3db/m3/tree/master/src/integration/resources/inprocess). Here's an example of starting up a DB node.

```golang
dbnode, _ := inprocess.NewDBNodeFromYAML(dbnodeCfg, inprocess.DBNodeOptions{})

```

## More Information
* [Some](https://github.com/m3db/m3/tree/master/src/integration/simple) [example](https://github.com/m3db/m3/tree/master/src/integration/repair) [tests](https://github.com/m3db/m3/tree/master/src/integration/prometheus)
* [Component APIs](https://github.com/m3db/m3/blob/1f98da6c2addca3001ff7b7b7a00a99a2c70bbbb/src/integration/resources/types.go#L55-L183)
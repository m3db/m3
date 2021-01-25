---
title: "Namespace Configuration"
weight: 8
---

Namespaces in M3DB are analogous to tables in other databases. Each namespace has a unique name as well as distinct configuration with regards to data retention and blocksize. For more information about namespaces and the technical details of their implementation, read our [storage engine documentation](/docs/m3db/architecture/engine).

## Namespace Operations

The operations below include sample cURLs, but you can always review the API documentation by navigating to

`http://<M3_COORDINATOR_HOST_NAME>:<CONFIGURED_PORT(default 7201)>/api/v1/openapi` or our [online API documentation](https://m3db.io/openapi/).

Additionally, the following headers can be used in the namespace operations: 

{{% fileinclude file="headers_placement_namespace.md" %}}

### Adding a Namespace

#### Recommended (Easy way)

The recommended way to add a namespace to M3DB is to use our `api/v1/database/namespace/create` endpoint. This API abstracts over a lot of the complexity of configuring a namespace and requires only two pieces of configuration to be provided: the name of the namespace, as well as its retention.

For example, the following cURL:

```shell
curl -X POST <M3_COORDINATOR_IP_ADDRESS>:<CONFIGURED_PORT(default 7201)>/api/v1/database/namespace/create -d '{
  "namespaceName": "default_unaggregated",
  "retentionTime": "24h"
}'
```

will create a namespace called `default_unaggregated` with a retention of `24 hours`. All of the other namespace options will either use reasonable default values or be calculated based on the provided `retentionTime`.

Once a namespace has finished bootstrapping, you must mark it as ready so that M3Coordinator knows the namespace is ready to receive reads and writes by using the _{{% apiendpoint %}}namespace/ready_.

{{< tabs name="ready_namespaces" >}}
{{% tab name="Command" %}}

{{< codeinclude file="docs/includes/operational_guide/ready-namespace.sh" language="shell" >}}

{{% /tab %}}
{{% tab name="Output" %}}

```json
{
  "ready": true
}
```

{{% /tab %}}
{{< /tabs >}}

If you feel the need to configure the namespace options yourself (for performance or other reasons), read the `Advanced` section below.

#### Advanced (Hard Way)

The "advanced" API allows you to configure every aspect of the namespace that you're adding which can sometimes be helpful for development, debugging, and tuning clusters for maximum performance.
Adding a namespace is a simple as using the `POST` `api/v1/services/m3db/namespace` API on an M3Coordinator instance.

```shell
curl -X POST <M3_COORDINATOR_IP_ADDRESS>:<CONFIGURED_PORT(default 7201)>/api/v1/services/m3db/namespace -d '{
  "name": "default_unaggregated",
  "options": {
    "bootstrapEnabled": true,
    "flushEnabled": true,
    "writesToCommitLog": true,
    "cleanupEnabled": true,
    "snapshotEnabled": true,
    "repairEnabled": false,
    "retentionOptions": {
      "retentionPeriodDuration": "2d",
      "blockSizeDuration": "2h",
      "bufferFutureDuration": "10m",
      "bufferPastDuration": "10m",
      "blockDataExpiry": true,
      "blockDataExpiryAfterNotAccessedPeriodDuration": "5m"
    },
    "indexOptions": {
      "enabled": true,
      "blockSizeDuration": "2h"
    },
    "aggregationOptions": {
      "aggregations": [
        { "aggregated": false }
      ]
    }
  }
}'
```

Once a namespace has finished bootstrapping, you must mark it as ready so that M3Coordinator knows the namespace is ready to receive reads and writes by using the _{{% apiendpoint %}}namespace/ready_.

{{< tabs name="ready_namespaces_adv" >}}
{{% tab name="Command" %}}

{{< codeinclude file="docs/includes/operational_guide/ready-namespace.sh" language="shell" >}}

{{% /tab %}}
{{% tab name="Output" %}}

```json
{
  "ready": true
}
```

{{% /tab %}}
{{< /tabs >}}

### Deleting a Namespace

Deleting a namespace is a simple as using the `DELETE` `/api/v1/services/m3db/namespace` API on an M3Coordinator instance.

`curl -X DELETE <M3_COORDINATOR_IP_ADDRESS>:<CONFIGURED_PORT(default 7201)>/api/v1/services/m3db/namespace/<NAMESPACE_NAME>`

Note that deleting a namespace will not have any effect on the M3DB nodes until they are all restarted.

### Modifying a Namespace

There is currently no atomic namespace modification endpoint. Instead, you will need to delete a namespace and then add it back again with the same name, but modified settings. Review the individual namespace settings above to determine whether or not a given setting is safe to modify. 

{{% notice warning %}}
For example, it is never safe to modify the blockSize of a namespace.
{{% /notice %}}

Also, be very careful not to restart the M3DB nodes after deleting the namespace, but before adding it back. If you do this, the M3DB nodes may detect the existing data files on disk and delete them since they are not configured to retain that namespace.

### Viewing a Namespace

In order to view a namespace and its attributes, use the `GET` `/api/v1/services/m3db/namespace` API on a M3Coordinator instance.
Additionally, for readability/debugging purposes, you can add the `debug=true` parameter to the URL to view block sizes, buffer sizes, etc.
in duration format as opposed to nanoseconds (default).

## Namespace Attributes

### bootstrapEnabled

This controls whether M3DB will attempt to [bootstrap](/docs/operational_guide/bootstrapping_crash_recovery) the namespace on startup. This value should always be set to `true` unless you have a very good reason to change it as setting it to `false` can cause data loss when restarting nodes.

Can be modified without creating a new namespace: `yes`

### flushEnabled

This controls whether M3DB will periodically flush blocks to disk once they become immutable. This value should always be set to `true` unless you have a very good reason to change it as setting it to `false` will cause increased memory utilization and potential data loss when restarting nodes.

Can be modified without creating a new namespace: `yes`

### writesToCommitlog

This controls whether M3DB will includes writes to this namespace in the commitlog. This value should always be set to `true` unless you have a very good reason to change it as setting it to `false` will cause potential data loss when restarting nodes.

Can be modified without creating a new namespace: `yes`

### snapshotEnabled

This controls whether M3DB will periodically write out [snapshot files](/docs/m3db/architecture/commitlogs) for this namespace which act as compacted commitlog files. This value should always be set to `true` unless you have a very good reason to change it as setting it to `false` will increasing bootstrapping times (reading commitlog files is slower than reading snapshot files) and increase disk utilization (snapshot files are compressed but commitlog files are uncompressed).

Can be modified without creating a new namespace: `yes`

### repairEnabled

If enabled, the M3DB nodes will attempt to compare the data they own with the data of their peers and emit metrics about any discrepancies. This feature is experimental and we do not recommend enabling it under any circumstances.

### retentionOptions

#### retentionPeriod

This controls the duration of time that M3DB will retain data for the namespace. For example, if this is set to 30 days, then data within this namespace will be available for querying up to 30 days after it is written. Note that this retention operates at the block level, not the write level, so its possible for individual datapoints to only be available for less than the specified retention. For example, if the blockSize was set to 24 hour and the retention was set to 30 days then a write that arrived at the very end of a 24 hour block would only be available for 29 days, but the node itself would always support querying the last 30 days worth of data.

Can be modified without creating a new namespace: `yes`

#### blockSize

This is the most important value to consider when tuning the performance of an M3DB namespace. Read the [storage engine documentation](/docs/m3db/architecture/storage) for more details, but the basic idea is that larger blockSizes will use more memory, but achieve higher compression. Similarly, smaller blockSizes will use less memory, but have worse compression. In testing, good compression occurs with blocksizes containing around 720 samples per timeseries.

Can be modified without creating a new namespace: `no`

Below are recommendations for block size based on resolution:

| Resolution | Block Size |
| ---------- | ---------- |
| 5s         | 60m        |
| 15s        | 3h         |
| 30s        | 6h         |
| 1m         | 12h        |
| 5m         | 60h        |

#### bufferFuture and bufferPast

These values control how far into the future and the past (compared to the system time on an M3DB node) writes for the namespace will be accepted. For example, consider the following configuration:

```text
bufferPast: 10m
bufferFuture: 20m
currentSystemTime: 2:35:00PM
```

Now consider the following writes (all of which arrive at 2:35:00PM system time, but include datapoints with the specified timestamps):

```text
2:25:00PM - Accepted, within the 10m bufferPast

2:24:59PM - Rejected, outside the 10m bufferPast

2:55:00PM - Accepted, within the 20m bufferFuture

2:55:01PM - Rejected, outside the 20m bufferFuture
```

While it may be tempting to configure `bufferPast` and `bufferFuture` to very large values to prevent writes from being rejected, this may cause performance issues. M3DB is a timeseries database that is optimized for realtime data. Out of order writes, as well as writes for times that are very far into the future or past are much more expensive and will cause additional CPU / memory pressure. In addition, M3DB cannot evict a block from memory until it is no longer mutable and large `bufferPast` and `bufferFuture` values effectively increase the amount of time that a block is mutable for which means that it must be kept in memory for a longer period of time.

Can be modified without creating a new namespace: `yes`

### indexOptions

#### enabled

Whether to use the built-in indexing. Must be `true`.

Can be modified without creating a new namespace: `no`

#### blockSize

The size of blocks (in duration) that the index uses.
Should match the databases [blocksize](#blocksize) for optimal memory usage.

Can be modified without creating a new namespace: `no`

### aggregationOptions
Options for the Coordinator to use to make decisions around how to aggregate datapoints.

Can be modified without creating a new namespace: `yes`

#### aggregations
One or more set of instructions on how datapoints should be aggregated within the namespace.

##### aggregated
Whether datapoints are aggregated.

##### attributes
If aggregated is true, specifies how to aggregate data.

###### resolutionNanos
The time range to aggregate data across.

###### downsampleOptions
Options related to downsampling data

###### _all_
Whether to send datapoints to this namespace. If false, the coordinator will not auto-aggregate incoming datapoints and datapoints must be sent the namespace via rules. Defaults to true.
module github.com/m3db/m3

go 1.16

require (
	github.com/CAFxX/gcnotifier v0.0.0-20190112062741-224a280d589d // indirect
	github.com/DataDog/datadog-go v3.7.1+incompatible // indirect
	github.com/MichaelTJones/pcg v0.0.0-20180122055547-df440c6ed7ed
	github.com/RoaringBitmap/roaring v0.4.21
	github.com/StackExchange/wmi v0.0.0-20210224194228-fe8f1750fd46 // indirect
	github.com/apache/thrift v0.14.2
	github.com/aws/aws-sdk-go v1.38.68 // indirect
	github.com/bmizerany/perks v0.0.0-20141205001514-d9a9656a3a4b // indirect
	github.com/c2h5oh/datasize v0.0.0-20171227191756-4eba002a5eae
	github.com/cenkalti/backoff/v3 v3.0.0
	github.com/cespare/xxhash/v2 v2.1.2
	github.com/containerd/continuity v0.0.0-20200413184840-d3ef23f19fbb // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/form3tech-oss/jwt-go v3.2.3+incompatible // indirect
	github.com/fortytw2/leaktest v1.3.0
	github.com/ghodss/yaml v1.0.0
	github.com/go-kit/kit v0.10.0
	github.com/go-playground/universal-translator v0.17.0 // indirect
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4
	github.com/google/go-cmp v0.5.7
	github.com/google/uuid v1.3.0
	github.com/gorilla/handlers v1.5.1 // indirect
	github.com/gorilla/mux v1.8.0
	github.com/hashicorp/hcl v1.0.1-0.20190611123218-cf7d376da96d // indirect
	github.com/hydrogen18/stalecucumber v0.0.0-20151102144322-9b38526d4bdf
	github.com/influxdata/influxdb v1.9.2
	github.com/jhump/protoreflect v1.6.1
	github.com/jonboulle/clockwork v0.2.2
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.14.2
	github.com/leanovate/gopter v0.2.8
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/lib/pq v1.9.0 // indirect
	github.com/lightstep/lightstep-tracer-go v0.18.1
	github.com/m3db/bitset v2.0.0+incompatible
	github.com/m3db/bloom/v4 v4.0.0-20200901140942-52efb8544fe9
	github.com/m3db/prometheus_client_golang v0.8.1
	github.com/m3db/prometheus_client_model v0.0.0-20180517145114-8b2299a4bf7d
	github.com/m3db/prometheus_common v0.0.0-20180517030744-25aaa3dff79b
	github.com/m3db/prometheus_procfs v0.8.1
	github.com/m3db/stackadler32 v0.0.0-20180104200216-bfebcd73ef6f
	github.com/m3db/stackmurmur3/v2 v2.0.2
	github.com/m3dbx/pilosa v1.4.1
	github.com/m3dbx/vellum v0.0.0-20201119082309-5b47f7a70f69
	github.com/mauricelam/genny v0.0.0-20180903214747-eb2c5232c885
	github.com/onsi/ginkgo v1.14.2 // indirect
	github.com/onsi/gomega v1.10.4 // indirect
	github.com/opentracing-contrib/go-stdlib v1.0.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/ory/dockertest/v3 v3.6.3
	github.com/pborman/getopt v0.0.0-20160216163137-ec82d864f599
	github.com/pborman/uuid v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/pkg/profile v1.2.1
	github.com/prashantv/protectmem v0.0.0-20171002184600-e20412882b3a // indirect
	github.com/prometheus/client_golang v1.12.1
	github.com/prometheus/common v0.32.1
	github.com/prometheus/prometheus v1.8.2-0.20210621150501-ff58416a0b02
	github.com/rakyll/statik v0.1.6
	github.com/sergi/go-diff v1.1.0
	github.com/shirou/gopsutil v3.21.6+incompatible // indirect
	github.com/spf13/cobra v1.3.0
	github.com/streadway/quantile v0.0.0-20150917103942-b0c588724d25 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/twmb/murmur3 v1.1.6 // indirect
	github.com/twotwotwo/sorts v0.0.0-20160814051341-bf5c1f2b8553
	github.com/uber-go/tally v3.4.3+incompatible
	github.com/uber/jaeger-client-go v2.29.1+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible
	github.com/uber/tchannel-go v1.20.1
	github.com/valyala/tcplisten v0.0.0-20161114210144-ceec8f93295a
	github.com/willf/bitset v1.1.10
	// etcd is currently on an alpha version to accomodate a GRPC version upgrade. See
	// https://github.com/m3db/m3/issues/4090 for the followup task to move back to a stable version.
	//  Gory details (why we're doing this):
	//
	//    - We import etcd/server/v3 via etcd/embed and etcd/testing/v3/frameworks/integration.
	//    - etcd/server/v3 in 3.5.2 depends on pre 1.0 opentelemetry. Bleeding edge etcd depends on 1.0 opentelemetry
	//    - M3 depends on 1.0 opentelemetry — this conflicts with etcd 3.5.2, but not bleeding edge etcd
	go.etcd.io/etcd/api/v3 v3.6.0-alpha.0
	go.etcd.io/etcd/client/pkg/v3 v3.6.0-alpha.0
	go.etcd.io/etcd/client/v3 v3.6.0-alpha.0
	go.etcd.io/etcd/server/v3 v3.6.0-alpha.0
	go.etcd.io/etcd/tests/v3 v3.6.0-alpha.0
	go.opentelemetry.io/collector v0.45.0
	go.opentelemetry.io/otel v1.4.1
	go.opentelemetry.io/otel/bridge/opentracing v1.4.1
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.4.1
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.4.1
	go.opentelemetry.io/otel/sdk v1.4.1
	go.uber.org/atomic v1.9.0
	go.uber.org/config v1.4.0
	go.uber.org/goleak v1.1.12
	go.uber.org/zap v1.21.0
	golang.org/x/net v0.0.0-20220105145211-5b0dc2dfae98
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20220114195835-da31bd327af9
	google.golang.org/grpc v1.44.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/go-playground/assert.v1 v1.2.1 // indirect
	gopkg.in/go-playground/validator.v9 v9.29.1
	gopkg.in/validator.v2 v2.0.0-20160201165114-3e4f037f12a1
	gopkg.in/vmihailenco/msgpack.v2 v2.8.3
	gopkg.in/yaml.v2 v2.4.0
)

// branch 0.9.3-patch
replace github.com/apache/thrift => github.com/m3dbx/thrift v0.0.0-20210326170526-6e3eef8b4a26

// NB(nate): upgrading to the latest msgpack is not backwards compatibile as msgpack will no longer attempt to automatically
// write an integer into the smallest number of bytes it will fit in. We rely on this behavior by having helper methods
// in at least two encoders (see below) take int64s and expect that msgpack will size them down accordingly. We'll have
// to make integer sizing explicit before attempting to upgrade.
//
// Encoders:
// src/metrics/encoding/msgpack/base_encoder.go
// src/dbnode/persist/fs/msgpack/encoder.go
replace gopkg.in/vmihailenco/msgpack.v2 => github.com/vmihailenco/msgpack v2.8.3+incompatible

replace github.com/stretchr/testify => github.com/stretchr/testify v1.1.4-0.20160305165446-6fe211e49392

replace github.com/google/flatbuffers => github.com/google/flatbuffers v1.12.1

// Fix legacy import path - https://github.com/uber-go/atomic/pull/60
replace github.com/uber-go/atomic => github.com/uber-go/atomic v1.4.0

replace github.com/envoyproxy/go-control-plane => github.com/envoyproxy/go-control-plane v0.9.7

replace google.golang.org/grpc => google.golang.org/grpc v1.40.1

// TODO: this can be removed once M3 is on Go 1.17.x
// This is here because of a dependency chain that makes client/v2 hard dependent on Go 1.17. Specifically:
//~ /Code/m3 $ go mod why sigs.k8s.io/json
// # sigs.k8s.io/json
// github.com/m3db/m3/src/aggregator/integration
// go.etcd.io/etcd/tests/v3/framework/integration
// go.etcd.io/etcd/server/v3/etcdserver
// go.etcd.io/etcd/server/v3/etcdserver/api/v2discovery
// go.etcd.io/etcd/client/v2
// sigs.k8s.io/json
// This dependency chain is recently added,
// (in https://github.com/etcd-io/etcd/commit/b5e4c2d3c483d9c3d03a0f97ae7b07607f38a5cc)
// and not actually relevant to anything used by M3 (we are on client/v3).
// Therefore, replace client/v2 to avoid the bad dependency chain until we upgrade.
replace go.etcd.io/etcd/client/v2 => go.etcd.io/etcd/client/v2 v2.305.0-alpha.0.0.20211029212747-6656181d312a

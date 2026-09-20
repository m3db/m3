module github.com/m3db/m3

go 1.25.6

require (
	github.com/MichaelTJones/pcg v0.0.0-20180122055547-df440c6ed7ed
	github.com/RoaringBitmap/roaring v0.4.21
	github.com/c2h5oh/datasize v0.0.0-20171227191756-4eba002a5eae
	github.com/cenkalti/backoff/v3 v3.2.2
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc
	github.com/fortytw2/leaktest v1.3.0
	github.com/ghodss/yaml v1.0.1-0.20190212211648-25d852aebe32
	github.com/go-kit/kit v0.13.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.7.0-rc.1
	github.com/golang/protobuf v1.5.4
	github.com/golang/snappy v1.0.0
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.1
	github.com/hydrogen18/stalecucumber v0.0.0-20180226003526-6de214d141dd
	github.com/influxdata/influxdb v1.9.5
	github.com/jhump/protoreflect v1.17.0
	github.com/jonboulle/clockwork v0.5.0
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.19.2
	github.com/leanovate/gopter v0.2.9
	github.com/lightstep/lightstep-tracer-go v0.18.1
	github.com/m3db/bitset v2.0.0+incompatible
	github.com/m3db/bloom/v4 v4.0.0-20200901140942-52efb8544fe9
	github.com/m3db/prometheus_client_golang v1.12.8
	github.com/m3db/prometheus_client_model v0.2.1
	github.com/m3db/prometheus_common v0.34.7
	github.com/m3db/prometheus_procfs v0.8.1
	github.com/m3db/stackadler32 v0.0.0-20180104200216-bfebcd73ef6f
	github.com/m3dbx/pilosa v1.4.1
	github.com/m3dbx/vellum v0.0.0-20201119082309-5b47f7a70f69
	github.com/mauricelam/genny v0.0.0-20190320071652-0800202903e5
	github.com/opentracing-contrib/go-stdlib v1.1.1
	github.com/opentracing/opentracing-go v1.2.1-0.20220228012449-10b1cf09e00b
	github.com/ory/dockertest/v3 v3.6.3
	github.com/pborman/getopt v0.0.0-20200816005738-fd0d075bf4de
	github.com/pborman/uuid v1.2.1
	github.com/pkg/errors v0.9.1
	github.com/pkg/profile v1.7.0
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/common v0.67.5
	github.com/prometheus/prometheus v0.0.0-20211110084043-4ef8c7c1d8e4
	github.com/rakyll/statik v0.1.7
	github.com/sergi/go-diff v1.4.0
	github.com/spf13/cobra v1.10.2
	github.com/stretchr/testify v1.12.1
	github.com/twmb/murmur3 v1.1.8
	github.com/twotwotwo/sorts v0.0.0-20160814051341-bf5c1f2b8553
	github.com/uber-go/tally v3.5.8+incompatible
	github.com/uber/jaeger-client-go v2.30.0+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible
	github.com/uber/tchannel-go v1.34.5
	github.com/valyala/tcplisten v1.0.0
	github.com/willf/bitset v1.1.11
	go.etcd.io/etcd/api/v3 v3.6.13
	go.etcd.io/etcd/client/pkg/v3 v3.6.13
	go.etcd.io/etcd/client/v3 v3.6.13
	go.etcd.io/etcd/server/v3 v3.6.13
	go.opentelemetry.io/otel v1.46.0
	go.opentelemetry.io/otel/bridge/opentracing v1.44.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.46.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.44.0
	go.opentelemetry.io/otel/sdk v1.46.0
	go.opentelemetry.io/proto/otlp v1.11.0
	go.uber.org/atomic v1.12.0
	go.uber.org/config v1.4.1
	go.uber.org/goleak v1.3.0
	go.uber.org/zap v1.28.0
	go.yaml.in/yaml/v3 v3.0.5
	golang.org/x/net v0.57.0
	golang.org/x/sync v0.22.0
	golang.org/x/sys v0.47.0
	google.golang.org/grpc v1.83.0
	google.golang.org/protobuf v1.36.12
	gopkg.in/go-playground/validator.v9 v9.31.0
	gopkg.in/validator.v2 v2.0.1
	gopkg.in/vmihailenco/msgpack.v2 v2.8.3
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/Azure/go-ansiterm v0.0.0-20250102033503-faa5f7b0171c // indirect
	github.com/CAFxX/gcnotifier v0.0.0-20190112062741-224a280d589d // indirect
	github.com/DataDog/datadog-go v3.7.1+incompatible // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/Nvveen/Gotty v0.0.0-20120604004816-cd527374f1e5 // indirect
	github.com/alecthomas/units v0.0.0-20240927000941-0f3dac36c52b // indirect
	github.com/aws/aws-sdk-go v1.55.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bufbuild/protocompile v0.14.2-0.20260414151636-6b2e7e07d2a7 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/containerd/continuity v0.4.5 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.7.0 // indirect
	github.com/dennwc/varint v1.0.0 // indirect
	github.com/docker/go-connections v0.7.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/edsrzf/mmap-go v1.2.0 // indirect
	github.com/felixge/fgprof v0.9.5 // indirect
	github.com/glycerine/go-unsnap-stream v0.0.0-20181221182339-f9677308dec2 // indirect
	github.com/go-kit/log v0.2.1 // indirect
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/pprof v0.0.0-20260402051712-545e8a4df936 // indirect
	github.com/gorilla/handlers v1.5.2 // indirect
	github.com/gorilla/websocket v1.5.4-0.20250319132907-e064f32e3674 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus v1.0.1 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.1.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/lightstep/lightstep-tracer-common/golang/gogo v0.0.0-20190605223551-bc2310a04743 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/moby/sys/user v0.4.0 // indirect
	github.com/moby/term v0.5.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/mschoch/smat v0.0.0-20160514031455-90eadee771ae // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/onsi/gomega v1.39.1 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/opencontainers/runc v1.3.3 // indirect
	github.com/petermattis/goid v0.0.0-20250319124200-ccd6737f222a // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common/sigv4 v0.1.0 // indirect
	github.com/prometheus/procfs v0.19.2 // indirect
	github.com/shirou/gopsutil v3.21.11+incompatible // indirect
	github.com/sirupsen/logrus v1.9.4 // indirect
	github.com/soheilhy/cmux v0.1.5 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	github.com/tinylib/msgp v1.6.4 // indirect
	github.com/tklauser/go-sysconf v0.4.0 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20220101234140-673ab2c3ae75 // indirect
	github.com/xiang90/probing v0.0.0-20221125231312-a49e3df8f510 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.etcd.io/bbolt v1.4.3 // indirect
	go.etcd.io/etcd/pkg/v3 v3.6.13 // indirect
	go.etcd.io/raft/v3 v3.6.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.70.0 // indirect
	go.opentelemetry.io/otel/metric v1.46.0 // indirect
	go.opentelemetry.io/otel/trace v1.46.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/lint v0.0.0-20241112194109-818c5a804067 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260720211330-0afa2a65878a // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260803160001-6ac0973c030d // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	sigs.k8s.io/json v0.0.0-20250730193827-2d320260d730 // indirect
	sigs.k8s.io/yaml v1.6.0 // indirect
)

// NB(nate): pinned to v2.8.3, which sizes an integer down to the smallest number of bytes it
// fits in. Newer msgpack does not: in v4 and v5, EncodeInt64/EncodeUint64 always write the full
// 9 bytes and the compact behavior moved to EncodeInt/EncodeUint. We depend on the compact
// encoding for the on-disk format via encodeVarint/encodeVarUint in
// src/dbnode/persist/fs/msgpack/encoder.go.
//
// The sharp edge is the commit log headers built at init in
// src/dbnode/persist/fs/msgpack/schema.go: DecodeLogEntryFast skips them by length without
// validating their contents, so widening the integers inside them would make new binaries
// silently misparse commit logs written by old ones. TestCommitLogHeadersUnchanged pins those
// bytes so that an upgrade fails there rather than during bootstrap.
//
// To upgrade, target v5 rather than v4 (v4's codes package is typed, v5's msgpcode constants
// are plain bytes like v2's, which the hand-inlined fast paths need). Switch
// encodeVarint/encodeVarUint to EncodeInt/EncodeUint, which are byte-for-byte identical to
// v2.8.3's EncodeInt64/EncodeUint64, and the on-disk format is preserved exactly.
//
// The replace below is separate from the pin: it rewrites the path at the same version, and the
// two module trees are identical, so it can be dropped on its own without touching the encoding.
replace gopkg.in/vmihailenco/msgpack.v2 => github.com/vmihailenco/msgpack v2.8.3+incompatible

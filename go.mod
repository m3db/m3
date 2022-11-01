module github.com/m3db/m3

go 1.19

require (
	github.com/MichaelTJones/pcg v0.0.0-20180122055547-df440c6ed7ed
	github.com/RoaringBitmap/roaring v0.4.21
	github.com/apache/thrift v0.13.0
	github.com/c2h5oh/datasize v0.0.0-20171227191756-4eba002a5eae
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/davecgh/go-spew v1.1.1
	github.com/fortytw2/leaktest v1.2.1-0.20180901000122-b433bbd6d743
	github.com/fossas/fossa-cli v1.0.30
	github.com/garethr/kubeval v0.0.0-20180821130434-c44f5193dc94
	github.com/ghodss/yaml v1.0.0
	github.com/go-kit/kit v0.10.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/mock v1.4.4
	github.com/golang/protobuf v1.4.2
	github.com/golang/snappy v0.0.1
	github.com/golangci/golangci-lint v1.33.0
	github.com/google/go-cmp v0.5.2
	github.com/google/go-jsonnet v0.16.0
	github.com/gorilla/mux v1.7.3
	github.com/hydrogen18/stalecucumber v0.0.0-20151102144322-9b38526d4bdf
	github.com/influxdata/influxdb v1.7.7
	github.com/jhump/protoreflect v1.6.1
	github.com/json-iterator/go v1.1.9
	github.com/leanovate/gopter v0.2.8
	github.com/lightstep/lightstep-tracer-go v0.18.1
	github.com/m3db/bitset v2.0.0+incompatible
	github.com/m3db/bloom/v4 v4.0.0-20200901140942-52efb8544fe9
	github.com/m3db/build-tools v0.0.0-20181013000606-edd1bdd1df8a
	github.com/m3db/m3x v0.0.0-20190408051622-ebf3c7b94afd
	github.com/m3db/prometheus_client_golang v0.8.1
	github.com/m3db/prometheus_client_model v0.0.0-20180517145114-8b2299a4bf7d
	github.com/m3db/prometheus_common v0.0.0-20180517030744-25aaa3dff79b
	github.com/m3db/prometheus_procfs v0.8.1
	github.com/m3db/stackadler32 v0.0.0-20180104200216-bfebcd73ef6f
	github.com/m3db/stackmurmur3/v2 v2.0.2
	github.com/m3db/tools v0.0.0-20181008195521-c6ded3f34878
	github.com/m3dbx/pilosa v1.4.2-0.20201109081833-6c9df43642fd
	github.com/m3dbx/vellum v0.0.0-20221006191556-bf692c9da731
	github.com/mauricelam/genny v0.0.0-20180903214747-eb2c5232c885
	github.com/mjibson/esc v0.1.0
	github.com/opentracing-contrib/go-stdlib v0.0.0-20190519235532-cf7a6c988dc9
	github.com/opentracing/opentracing-go v1.2.0
	github.com/ory/dockertest/v3 v3.6.3
	github.com/pborman/getopt v0.0.0-20160216163137-ec82d864f599
	github.com/pborman/uuid v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/pkg/profile v1.2.1
	github.com/pointlander/peg v1.0.0
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/common v0.9.1
	github.com/prometheus/prometheus v1.8.2-0.20200420081721-18254838fbe2
	github.com/rakyll/statik v0.1.6
	github.com/sergi/go-diff v1.1.0
	github.com/spf13/cobra v1.1.1
	github.com/stretchr/testify v1.6.1
	github.com/twotwotwo/sorts v0.0.0-20160814051341-bf5c1f2b8553
	github.com/uber-go/tally v3.3.13+incompatible
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	github.com/uber/jaeger-lib v2.2.0+incompatible
	github.com/uber/tchannel-go v1.14.0
	github.com/valyala/tcplisten v0.0.0-20161114210144-ceec8f93295a
	github.com/willf/bitset v1.1.10
	github.com/wjdp/htmltest v0.13.0

	// This is 3.4.13. Note: we need to specify the version this way due to the issue
	// described in https://github.com/etcd-io/etcd/issues/11154 .
	// Version string was obtained by the method described in
	// https://github.com/etcd-io/etcd/issues/11154#issuecomment-568587798
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200824191128-ae9734ed278b
	go.uber.org/atomic v1.6.0
	go.uber.org/config v1.4.0
	go.uber.org/goleak v1.1.10
	go.uber.org/zap v1.13.0
	golang.org/x/net v0.0.0-20200822124328-c89045814202
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/sys v0.0.0-20201009025420-dfb3f7c4e634
	golang.org/x/tools v0.0.0-20201013201025-64a9e34f3752
	google.golang.org/grpc v1.29.1
	google.golang.org/protobuf v1.23.0
	gopkg.in/go-playground/validator.v9 v9.7.0
	gopkg.in/validator.v2 v2.0.0-20160201165114-3e4f037f12a1
	gopkg.in/vmihailenco/msgpack.v2 v2.8.3
	gopkg.in/yaml.v2 v2.3.0
	gotest.tools v2.2.0+incompatible
)

require (
	4d63.com/gochecknoglobals v0.0.0-20201008074935-acfc0b28355a // indirect
	github.com/Azure/go-ansiterm v0.0.0-20170929234023-d6e3b3328b78 // indirect
	github.com/BurntSushi/toml v0.3.1 // indirect
	github.com/CAFxX/gcnotifier v0.0.0-20190112062741-224a280d589d // indirect
	github.com/DataDog/datadog-go v3.7.1+incompatible // indirect
	github.com/Djarvur/go-err113 v0.0.0-20200511133814-5174e21577d5 // indirect
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/Microsoft/go-winio v0.4.14 // indirect
	github.com/Nvveen/Gotty v0.0.0-20120604004816-cd527374f1e5 // indirect
	github.com/OpenPeeDeeP/depguard v1.0.1 // indirect
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/apex/log v1.3.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver v3.5.1+incompatible // indirect
	github.com/bmatcuk/doublestar v1.3.1 // indirect
	github.com/bmizerany/perks v0.0.0-20141205001514-d9a9656a3a4b // indirect
	github.com/bombsimon/wsl/v3 v3.1.0 // indirect
	github.com/briandowns/spinner v1.11.1 // indirect
	github.com/cenkalti/backoff/v3 v3.0.0 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cheekybits/genny v1.0.0 // indirect
	github.com/containerd/continuity v0.0.0-20200413184840-d3ef23f19fbb // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190321100706-95778dfbb74e // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/daixiang0/gci v0.2.4 // indirect
	github.com/denis-tingajkin/go-header v0.3.1 // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/docopt/docopt-go v0.0.0-20180111231733-ee0de3bc6815 // indirect
	github.com/dustin/go-humanize v0.0.0-20171111073723-bb3d318650d4 // indirect
	github.com/edsrzf/mmap-go v1.0.0 // indirect
	github.com/emirpasic/gods v1.12.0 // indirect
	github.com/fatih/color v1.10.0 // indirect
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/glycerine/go-unsnap-stream v0.0.0-20181221182339-f9677308dec2 // indirect
	github.com/gnewton/jargo v0.0.0-20150417131352-41f5f186a805 // indirect
	github.com/go-critic/go-critic v0.5.2 // indirect
	github.com/go-logfmt/logfmt v0.5.0 // indirect
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/go-playground/locales v0.13.0 // indirect
	github.com/go-playground/universal-translator v0.17.0 // indirect
	github.com/go-toolsmith/astcast v1.0.0 // indirect
	github.com/go-toolsmith/astcopy v1.0.0 // indirect
	github.com/go-toolsmith/astequal v1.0.0 // indirect
	github.com/go-toolsmith/astfmt v1.0.0 // indirect
	github.com/go-toolsmith/astp v1.0.0 // indirect
	github.com/go-toolsmith/strparse v1.0.0 // indirect
	github.com/go-toolsmith/typep v1.0.2 // indirect
	github.com/go-xmlfmt/xmlfmt v0.0.0-20191208150333-d5b6f63a941b // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/gofrs/flock v0.8.0 // indirect
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/golangci/check v0.0.0-20180506172741-cfe4005ccda2 // indirect
	github.com/golangci/dupl v0.0.0-20180902072040-3e9179ac440a // indirect
	github.com/golangci/errcheck v0.0.0-20181223084120-ef45e06d44b6 // indirect
	github.com/golangci/go-misc v0.0.0-20180628070357-927a3d87b613 // indirect
	github.com/golangci/gocyclo v0.0.0-20180528144436-0a533e8fa43d // indirect
	github.com/golangci/gofmt v0.0.0-20190930125516-244bba706f1a // indirect
	github.com/golangci/ineffassign v0.0.0-20190609212857-42439a7714cc // indirect
	github.com/golangci/lint-1 v0.0.0-20191013205115-297bf364a8e0 // indirect
	github.com/golangci/maligned v0.0.0-20180506175553-b1d89398deca // indirect
	github.com/golangci/misspell v0.0.0-20180809174111-950f5d19e770 // indirect
	github.com/golangci/prealloc v0.0.0-20180630174525-215b22d4de21 // indirect
	github.com/golangci/revgrep v0.0.0-20180526074752-d9c87f5ffaf0 // indirect
	github.com/golangci/unconvert v0.0.0-20180507085042-28b1c447d1f4 // indirect
	github.com/google/btree v1.0.0 // indirect
	github.com/google/go-github/v30 v30.1.0 // indirect
	github.com/google/go-querystring v1.0.0 // indirect
	github.com/google/uuid v1.1.2-0.20190416172445-c2e93f3ae59f // indirect
	github.com/gorilla/handlers v1.4.2 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/gostaticanalysis/analysisutil v0.1.0 // indirect
	github.com/gostaticanalysis/comment v1.3.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.1-0.20190118093823-f849b5445de4 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.14.1 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.0 // indirect
	github.com/hashicorp/hcl v1.0.1-0.20190611123218-cf7d376da96d // indirect
	github.com/imdario/mergo v0.3.8 // indirect
	github.com/inconshreveable/go-update v0.0.0-20160112193335-8152e7eb6ccf // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jbenet/go-context v0.0.0-20150711004518-d14ea06fba99 // indirect
	github.com/jgautheron/goconst v0.0.0-20201117150253-ccae5bf973f3 // indirect
	github.com/jingyugao/rowserrcheck v0.0.0-20191204022205-72ab7603b68a // indirect
	github.com/jirfag/go-printf-func-name v0.0.0-20191110105641-45db9963cdd3 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/kevinburke/ssh_config v0.0.0-20190725054713-01f96b0aa0cd // indirect
	github.com/kisielk/gotool v1.0.0 // indirect
	github.com/kunwardeep/paralleltest v1.0.2 // indirect
	github.com/kyoh86/exportloopref v0.1.8 // indirect
	github.com/lib/pq v1.6.0 // indirect
	github.com/lightstep/lightstep-tracer-common/golang/gogo v0.0.0-20190605223551-bc2310a04743 // indirect
	github.com/magiconair/properties v1.8.2 // indirect
	github.com/maratori/testpackage v1.0.1 // indirect
	github.com/matoous/godox v0.0.0-20190911065817-5d6d842e92eb // indirect
	github.com/mattn/go-colorable v0.1.8 // indirect
	github.com/mattn/go-isatty v0.0.12 // indirect
	github.com/mattn/go-runewidth v0.0.2 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mbilski/exhaustivestruct v1.1.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/go-wordwrap v1.0.0 // indirect
	github.com/mitchellh/mapstructure v1.3.3 // indirect
	github.com/moby/term v0.0.0-20200915141129-7f0af18e79f2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/moricho/tparallel v0.2.1 // indirect
	github.com/mschoch/smat v0.0.0-20160514031455-90eadee771ae // indirect
	github.com/nakabonne/nestif v0.3.0 // indirect
	github.com/nbutton23/zxcvbn-go v0.0.0-20180912185939-ae427f1e4c1d // indirect
	github.com/nishanths/exhaustive v0.1.0 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/olekukonko/tablewriter v0.0.0-20170122224234-a0225b3f23b5 // indirect
	github.com/opencontainers/go-digest v1.0.0-rc1 // indirect
	github.com/opencontainers/image-spec v1.0.1 // indirect
	github.com/opencontainers/runc v1.0.0-rc9 // indirect
	github.com/pelletier/go-toml v1.5.0 // indirect
	github.com/phayes/checkstyle v0.0.0-20170904204023-bfd46e6a821d // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/pointlander/compress v1.1.0 // indirect
	github.com/pointlander/jetset v1.0.0 // indirect
	github.com/polyfloyd/go-errorlint v0.0.0-20201006195004-351e25ade6e3 // indirect
	github.com/prashantv/protectmem v0.0.0-20171002184600-e20412882b3a // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/procfs v0.0.8 // indirect
	github.com/quasilyte/go-ruleguard v0.2.0 // indirect
	github.com/quasilyte/regex/syntax v0.0.0-20200407221936-30656e2c4a95 // indirect
	github.com/remeh/sizedwaitgroup v1.0.0 // indirect
	github.com/rhysd/go-github-selfupdate v1.2.2 // indirect
	github.com/russross/blackfriday/v2 v2.0.1 // indirect
	github.com/rveen/ogdl v0.0.0-20200522080342-eeeda1a978e7 // indirect
	github.com/ryancurrah/gomodguard v1.1.0 // indirect
	github.com/ryanrolds/sqlclosecheck v0.3.0 // indirect
	github.com/securego/gosec/v2 v2.5.0 // indirect
	github.com/shazow/go-diff v0.0.0-20160112020656-b6b7b6733b8c // indirect
	github.com/shirou/gopsutil v2.20.5+incompatible // indirect
	github.com/shurcooL/sanitized_anchor_name v1.0.0 // indirect
	github.com/sirupsen/logrus v1.7.0 // indirect
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/sonatard/noctx v0.0.1 // indirect
	github.com/sourcegraph/go-diff v0.6.1 // indirect
	github.com/spf13/afero v1.2.2 // indirect
	github.com/spf13/cast v1.3.1-0.20190531151931-f31dc0aaab5a // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/spf13/viper v1.7.1 // indirect
	github.com/src-d/gcfg v1.4.0 // indirect
	github.com/ssgreg/nlreturn/v2 v2.1.0 // indirect
	github.com/streadway/quantile v0.0.0-20150917103942-b0c588724d25 // indirect
	github.com/stretchr/objx v0.2.0 // indirect
	github.com/subosito/gotenv v1.2.1-0.20190917103637-de67a6614a4d // indirect
	github.com/tcnksm/go-gitconfig v0.1.2 // indirect
	github.com/tdakkota/asciicheck v0.0.0-20200416190851-d7f85be797a2 // indirect
	github.com/tetafro/godot v1.3.0 // indirect
	github.com/timakin/bodyclose v0.0.0-20190930140734-f7f2e9bca95e // indirect
	github.com/tinylib/msgp v1.1.0 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/tomarrell/wrapcheck v0.0.0-20200807122107-df9e8bcb914d // indirect
	github.com/tommy-muehle/go-mnd v1.3.1-0.20200224220436-e6f9a994e8fa // indirect
	github.com/ulikunitz/xz v0.5.5 // indirect
	github.com/ultraware/funlen v0.0.3 // indirect
	github.com/ultraware/whitespace v0.0.4 // indirect
	github.com/urfave/cli v1.22.1 // indirect
	github.com/uudashr/gocognit v1.0.1 // indirect
	github.com/xanzy/ssh-agent v0.2.1 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20180127040702-4e3ac2762d5f // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/bbolt v1.3.5 // indirect
	go.uber.org/multierr v1.4.0 // indirect
	go.uber.org/tools v0.0.0-20190618225709-2cfd321de3ee // indirect
	golang.org/x/crypto v0.0.0-20200622213623-75b288015ac9 // indirect
	golang.org/x/lint v0.0.0-20190930215403-16217165b5de // indirect
	golang.org/x/mod v0.3.0 // indirect
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d // indirect
	golang.org/x/text v0.3.3 // indirect
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/appengine v1.6.1 // indirect
	google.golang.org/genproto v0.0.0-20200305110556-506484158171 // indirect
	gopkg.in/go-ini/ini.v1 v1.57.0 // indirect
	gopkg.in/go-playground/assert.v1 v1.2.1 // indirect
	gopkg.in/ini.v1 v1.51.1 // indirect
	gopkg.in/seborama/govcr.v2 v2.4.2 // indirect
	gopkg.in/src-d/go-billy.v4 v4.3.2 // indirect
	gopkg.in/src-d/go-git.v4 v4.13.1 // indirect
	gopkg.in/warnings.v0 v0.1.2 // indirect
	honnef.co/go/tools v0.0.1-2020.1.6 // indirect
	mvdan.cc/gofumpt v0.0.0-20200802201014-ab5a8192947d // indirect
	mvdan.cc/interfacer v0.0.0-20180901003855-c20040233aed // indirect
	mvdan.cc/lint v0.0.0-20170908181259-adc824a0674b // indirect
	mvdan.cc/unparam v0.0.0-20200501210554-b37ab49443f7 // indirect
	sigs.k8s.io/yaml v1.1.0 // indirect
)

// branch 0.9.3-pool-read-binary-3
replace github.com/apache/thrift => github.com/m3db/thrift v0.0.0-20190820191926-05b5a2227fe4

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

replace github.com/prometheus/common => github.com/prometheus/common v0.9.1

// Fix legacy import path - https://github.com/uber-go/atomic/pull/60
replace github.com/uber-go/atomic => github.com/uber-go/atomic v1.4.0

// Pull in https://github.com/etcd-io/bbolt/pull/220, required for go 1.14 compatibility
//
// etcd 3.14.13 depends on v1.3.3, but everything before v1.3.5 has unsafe misuses, and fails hard on go 1.14
// TODO: remove after etcd pulls in the change to a new release on 3.4 branch
replace go.etcd.io/bbolt => go.etcd.io/bbolt v1.3.5

// https://github.com/ory/dockertest/issues/212
replace golang.org/x/sys => golang.org/x/sys v0.1.0

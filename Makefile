SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/.ci/common.mk

SHELL=/bin/bash -o pipefail

auto_gen             := .ci/auto-gen.sh
gopath_prefix        := $(GOPATH)/src
license_dir          := .ci/uber-licence
license_node_modules := $(license_dir)/node_modules
m3db_package         := github.com/m3db/m3db
m3db_package_path    := $(gopath_prefix)/$(m3db_package)
metalint_check       := .ci/metalint.sh
metalint_config      := .metalinter.json
metalint_exclude     := .excludemetalint
mockgen_package      := github.com/golang/mock/mockgen
mocks_output_dir     := generated/mocks/mocks
mocks_rules_dir      := generated/mocks
proto_output_dir     := generated/proto
proto_rules_dir      := generated/proto
protoc_go_package    := github.com/golang/protobuf/protoc-gen-go
thrift_gen_package   := github.com/uber/tchannel-go
thrift_output_dir    := generated/thrift/rpc
thrift_rules_dir     := generated/thrift
m3x_package          := github.com/m3db/m3x
m3x_package_path     := $(gopath_prefix)/$(m3x_package)
m3x_package_min_ver  := d0873c0b327bee1a4a47f1d84263a6edaebbbf66
vendor_prefix        := vendor
cache_policy         ?= recently_read

BUILD            := $(abspath ./bin)
GO_BUILD_LDFLAGS := $(shell $(abspath ./.ci/go-build-ldflags.sh) $(m3db_package))
LINUX_AMD64_ENV  := GOOS=linux GOARCH=amd64 CGO_ENABLED=0

SERVICES := \
	m3dbnode

TOOLS :=            \
	read_ids          \
	read_index_ids    \
	read_data_files   \
	clone_fileset     \
	dtest             \
	verify_commitlogs \
	verify_index_files

.PHONY: setup
setup:
	mkdir -p $(BUILD)

define SERVICE_RULES

.PHONY: $(SERVICE)
$(SERVICE): setup
	@echo Building $(SERVICE)
	go build -ldflags '$(GO_BUILD_LDFLAGS)' -o $(BUILD)/$(SERVICE) ./services/$(SERVICE)/main/.

.PHONY: $(SERVICE)-linux-amd64
$(SERVICE)-linux-amd64:
	$(LINUX_AMD64_ENV) make $(SERVICE)

endef

define TOOL_RULES

.PHONY: $(TOOL)
$(TOOL): setup
	@echo Building $(TOOL)
	go build -o $(BUILD)/$(TOOL) ./tools/$(TOOL)/main/.

.PHONY: $(TOOL)-linux-amd64
$(TOOL)-linux-amd64:
	$(LINUX_AMD64_ENV) make $(TOOL)

endef

.PHONY: services services-linux-amd64
services: $(SERVICES)
services-linux-amd64:
	$(LINUX_AMD64_ENV) make services

.PHONY: tools tools-linux-amd64
tools: $(TOOLS)
tools-linux-amd64:
	$(LINUX_AMD64_ENV) make tools

$(foreach SERVICE,$(SERVICES),$(eval $(SERVICE_RULES)))
$(foreach TOOL,$(TOOLS),$(eval $(TOOL_RULES)))

.PHONY: all
all: metalint test-ci-unit test-ci-integration services tools
	@echo Made all successfully

.PHONY: install-license-bin
install-license-bin:
	@echo Installing node modules
	[ -d $(license_node_modules) ] || (          \
		git submodule update --init --recursive && \
		cd $(license_dir) && npm install           \
	)

.PHONY: install-mockgen
install-mockgen:
	@echo Installing mockgen
	@which mockgen >/dev/null || (make install-vendor                               && \
		rm -rf $(gopath_prefix)/$(mockgen_package)                                    && \
		cp -r $(vendor_prefix)/$(mockgen_package) $(gopath_prefix)/$(mockgen_package) && \
		go install $(mockgen_package)                                                    \
	)

.PHONY: install-thrift-bin
install-thrift-bin: install-glide
	@echo Installing thrift binaries
	@echo Note: the thrift binary should be installed from https://github.com/apache/thrift at commit 9b954e6a469fef18682314458e6fc4af2dd84add.
	@which thrift-gen >/dev/null || (make install-vendor                                      && \
		go get $(thrift_gen_package) && cd $(GOPATH)/src/$(thrift_gen_package) && glide install && \
		go install $(thrift_gen_package)/thrift/thrift-gen                                         \
	)

.PHONY: install-proto-bin
install-proto-bin: install-glide
	@echo Installing protobuf binaries
	@echo Note: the protobuf compiler v3.0.0 can be downloaded from https://github.com/google/protobuf/releases or built from source at https://github.com/google/protobuf.
	@which protoc-gen-go >/dev/null || (make install-vendor            && \
		go install $(m3db_package)/$(vendor_prefix)/$(protoc_go_package)    \
	)

.PHONY: mock-gen
mock-gen: install-mockgen install-license-bin install-util-mockclean
	@echo Generating mocks
	PACKAGE=$(m3db_package) $(auto_gen) $(mocks_output_dir) $(mocks_rules_dir)

.PHONY: thrift-gen
thrift-gen: install-thrift-bin install-license-bin
	@echo Generating thrift files
	PACKAGE=$(m3db_package) $(auto_gen) $(thrift_output_dir) $(thrift_rules_dir)

.PHONY: proto-gen
proto-gen: install-proto-bin install-license-bin
	@echo Generating protobuf files
	PACKAGE=$(m3db_package) $(auto_gen) $(proto_output_dir) $(proto_rules_dir)

.PHONY: install-m3x-repo
install-m3x-repo: install-glide install-generics-bin
	# Check if repository exists, if not get it
	test -d $(m3x_package_path) && test -d $(m3x_package_path)/vendor || ( \
		go get -u $(m3x_package)                                          && \
		cd $(m3x_package_path)                                            && \
		glide install                                                        \
	)
	# If does exist but not at min version then update it
	(cd $(m3x_package_path) && git cat-file -t $(m3x_package_min_ver) > /dev/null) || go get -u $(m3x_package)

# Map generation rule for client/receivedBlocksMap
.PHONY: map-client-received-blocks-gen
map-client-received-blocks-gen: install-m3x-repo
	cd $(m3x_package_path) && make hashmap-gen \
		pkg=client \
		key_type=idAndBlockStart \
		value_type=receivedBlocks \
		out_dir=$(m3db_package_path)/client \
		rename_type_prefix=receivedBlocks
	# Rename generated map file
	mv -f $(m3db_package_path)/client/map_gen.go $(m3db_package_path)/client/received_blocks_map_gen.go

# Map generation rule for storage/block/retrieverMap
.PHONY: map-storage-block-retriever-gen
map-storage-block-retriever-gen: install-m3x-repo
	cd $(m3x_package_path) && make idhashmap-gen \
		pkg=block \
		value_type=DatabaseBlockRetriever \
		out_dir=$(m3db_package_path)/storage/block \
		rename_type_prefix=retriever \
		rename_constructor=newRetrieverMap \
		rename_constructor_options=retrieverMapOptions
	# Rename both generated map and constructor files
	mv -f $(m3db_package_path)/storage/block/map_gen.go $(m3db_package_path)/storage/block/retriever_map_gen.go
	mv -f $(m3db_package_path)/storage/block/new_map_gen.go $(m3db_package_path)/storage/block/retriever_new_map_gen.go

# Map generation rule for storage/bootstrap/result/Map
.PHONY: map-storage-bootstrap-result-gen
map-storage-bootstrap-result-gen: install-m3x-repo
	cd $(m3x_package_path) && make idhashmap-gen \
		pkg=result \
		value_type=DatabaseSeriesBlocks \
		out_dir=$(m3db_package_path)/storage/bootstrap/result

# Map generation rule for storage package maps (to avoid double build over each other
# when generating map source files in parallel, run these sequentially)
.PHONY: map-storage-gen
map-storage-gen:
	make map-storage-database-namespaces-gen
	make map-storage-shard-gen

# Map generation rule for storage/databaseNamespacesMap
.PHONY: map-storage-database-namespaces-gen
map-storage-database-namespaces-gen: install-m3x-repo
	cd $(m3x_package_path) && make idhashmap-gen \
		pkg=storage \
		value_type=databaseNamespace \
		out_dir=$(m3db_package_path)/storage \
		rename_type_prefix=databaseNamespaces \
		rename_constructor=newDatabaseNamespacesMap \
		rename_constructor_options=databaseNamespacesMapOptions
	# Rename both generated map and constructor files
	mv -f $(m3db_package_path)/storage/map_gen.go $(m3db_package_path)/storage/namespace_map_gen.go
	mv -f $(m3db_package_path)/storage/new_map_gen.go $(m3db_package_path)/storage/namespace_new_map_gen.go

# Map generation rule for storage/shardMap
.PHONY: map-storage-shard-gen
map-storage-shard-gen: install-m3x-repo
	cd $(m3x_package_path) && make idhashmap-gen \
		pkg=storage \
		value_type=listElement \
		out_dir=$(m3db_package_path)/storage \
		rename_type_prefix=shard \
		rename_constructor=newShardMap \
		rename_constructor_options=shardMapOptions
	# Rename both generated map and constructor files
	mv -f $(m3db_package_path)/storage/map_gen.go $(m3db_package_path)/storage/shard_map_gen.go
	mv -f $(m3db_package_path)/storage/new_map_gen.go $(m3db_package_path)/storage/shard_new_map_gen.go

# Map generation rule for storage/namespace/metadataMap
.PHONY: map-storage-namespace-metadata-gen
map-storage-namespace-metadata-gen: install-m3x-repo
	cd $(m3x_package_path) && make idhashmap-gen \
		pkg=namespace \
		value_type=Metadata \
		out_dir=$(m3db_package_path)/storage/namespace \
		rename_type_prefix=metadata \
		rename_constructor=newMetadataMap \
		rename_constructor_options=metadataMapOptions
	# Rename both generated map and constructor files
	mv -f $(m3db_package_path)/storage/namespace/map_gen.go $(m3db_package_path)/storage/namespace/metadata_map_gen.go
	mv -f $(m3db_package_path)/storage/namespace/new_map_gen.go $(m3db_package_path)/storage/namespace/metadata_new_map_gen.go

# Map generation rule for storage/repair/Map
.PHONY: map-storage-repair-gen
map-storage-repair-gen: install-m3x-repo
	cd $(m3x_package_path) && make idhashmap-gen \
		pkg=repair \
		value_type=ReplicaSeriesBlocksMetadata \
		out_dir=$(m3db_package_path)/storage/repair

# Map generation rule for all generated maps
.PHONY: map-all-gen
map-all-gen: map-client-received-blocks-gen map-storage-block-retriever-gen map-storage-bootstrap-result-gen map-storage-gen map-storage-namespace-metadata-gen map-storage-repair-gen

# Tests that all currently generated maps match their contents if they were regenerated now
.PHONY: test-map-all-gen
test-map-all-gen: map-all-gen
	@test "$(shell git diff --shortstat 2> /dev/null)" = "" || (echo "Check git status, there are dirty files" && exit 1)
	@test "$(shell git status --porcelain 2>/dev/null | grep "^??")" = "" || (echo "Check git status, there are untracked files" && exit 1)

.PHONY: all-gen
# NB(prateek): order matters here, mock-gen needs to be last because we sometimes
# generate mocks for thrift/proto generated code.
all-gen: thrift-gen proto-gen mock-gen map-all-gen

.PHONY: metalint
metalint: install-metalinter install-linter-badtime
	@($(metalint_check) $(metalint_config) $(metalint_exclude))

.PHONY: test
test: test-base
	# coverfile defined in common.mk
	gocov convert $(coverfile) | gocov report

.PHONY: test-xml
test-xml: test-base-xml

.PHONY: test-html
test-html: test-base-html

# Note: do not test native pooling since it's experimental/deprecated
.PHONY: test-integration
test-integration:
	TEST_NATIVE_POOLING=false make test-base-integration

# Usage: make test-single-integration name=<test_name>
.PHONY: test-single-integration
test-single-integration:
	TEST_NATIVE_POOLING=false make test-base-single-integration name=$(name)

.PHONY: test-ci-unit
test-ci-unit: test-base
	$(codecov_push) -f $(coverfile) -F unittests

.PHONY: test-ci-big-unit
test-ci-big-unit: test-big-base
	$(codecov_push) -f $(coverfile) -F unittests

.PHONY: test-ci-integration
test-ci-integration:
	INTEGRATION_TIMEOUT=4m TEST_NATIVE_POOLING=false TEST_SERIES_CACHE_POLICY=$(cache_policy) make test-base-ci-integration
	$(codecov_push) -f $(coverfile) -F integration

.PHONY: clean
clean:
	@rm -f *.html *.xml *.out *.test

.DEFAULT_GOAL := all

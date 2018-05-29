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
mocks_output_dir     := generated/mocks
mocks_rules_dir      := generated/mocks
proto_output_dir     := generated/proto
proto_rules_dir      := generated/proto
protoc_go_package    := github.com/golang/protobuf/protoc-gen-go
thrift_gen_package   := github.com/uber/tchannel-go
thrift_output_dir    := generated/thrift/rpc
thrift_rules_dir     := generated/thrift
vendor_prefix        := vendor
cache_policy         ?= recently_read

BUILD                := $(abspath ./bin)
GO_BUILD_LDFLAGS_CMD := $(abspath ./.ci/go-build-ldflags.sh) $(m3db_package)
GO_BUILD_LDFLAGS     := $(shell $(GO_BUILD_LDFLAGS_CMD))
LINUX_AMD64_ENV      := GOOS=linux GOARCH=amd64 CGO_ENABLED=0
GO_RELEASER_VERSION  := v0.76.1

include $(SELF_DIR)/src/dbnode/generated-source-files.mk

SERVICES :=     \
	m3dbnode      \
	m3coordinator

SUBDIRS :=    \
	cmd         \
	dbnode      \
	coordinator

TOOLS :=            \
	read_ids          \
	read_index_ids    \
	read_data_files   \
	read_index_files  \
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
	go build -ldflags '$(GO_BUILD_LDFLAGS)' -o $(BUILD)/$(SERVICE) ./src/cmd/services/$(SERVICE)/main/.

.PHONY: $(SERVICE)-linux-amd64
$(SERVICE)-linux-amd64:
	$(LINUX_AMD64_ENV) make $(SERVICE)

endef

$(foreach SERVICE,$(SERVICES),$(eval $(SERVICE_RULES)))

define TOOL_RULES

.PHONY: $(TOOL)
$(TOOL): setup
	@echo Building $(TOOL)
	go build -o $(BUILD)/$(TOOL) ./src/cmd/tools/$(TOOL)/main/.

.PHONY: $(TOOL)-linux-amd64
$(TOOL)-linux-amd64:
	$(LINUX_AMD64_ENV) make $(TOOL)

endef

$(foreach TOOL,$(TOOLS),$(eval $(TOOL_RULES)))

.PHONY: services services-linux-amd64
services: $(SERVICES)
services-linux-amd64:
	$(LINUX_AMD64_ENV) make services

.PHONY: tools tools-linux-amd64
tools: $(TOOLS)
tools-linux-amd64:
	$(LINUX_AMD64_ENV) make tools

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

install-stringer:
		@which stringer > /dev/null || go get golang.org/x/tools/cmd/stringer
		@which stringer > /dev/null || (echo "stringer install failed" && exit 1)

install-goreleaser: install-stringer
		@which goreleaser > /dev/null || (go get -d github.com/goreleaser/goreleaser && \
		  cd $(GOPATH)/src/github.com/goreleaser/goreleaser && \
			git checkout $(GO_RELEASER_VERSION) && \
			dep ensure -vendor-only && \
			make build && \
			mv goreleaser $(GOPATH)/bin/)
		@goreleaser -version 2> /dev/null || (echo "goreleaser install failed" && exit 1)

.PHONY: release
release: install-goreleaser
	@echo Releasing new version
	@source $(GO_BUILD_LDFLAGS_CMD) > /dev/null && goreleaser

.PHONY: release-snapshot
release-snapshot: install-goreleaser
	@echo Creating snapshot release
	@source $(GO_BUILD_LDFLAGS_CMD) > /dev/null && goreleaser --snapshot --rm-dist


.PHONY: docs-container
docs-container:
	which docker
	docker build -t m3db-docs -f src/scripts/Dockerfile-docs docs

.PHONY: docs-build
docs-build: docs-container
	docker run -v $(PWD):/m3db --rm m3db-docs "mkdocs build -e docs/theme -t material"

.PHONY: docs-serve
docs-serve: docs-container
	docker run -v $(PWD):/m3db -p 8000:8000 -it --rm m3db-docs "mkdocs serve -e docs/theme -t material -a 0.0.0.0:8000"

.PHONY: docs-deploy
docs-deploy: docs-container
	docker run -v $(PWD):/m3db --rm m3db-docs "mkdocs build -e docs/theme -t material && mkdocs gh-deploy --dirty"

define SUBDIR_RULES

.PHONY: mock-gen-$(SUBDIR)
mock-gen-$(SUBDIR): install-mockgen install-license-bin install-util-mockclean
	@echo Generating mocks
	PACKAGE=$(m3db_package) $(auto_gen) src/$(SUBDIR)/$(mocks_output_dir) src/$(SUBDIR)/$(mocks_rules_dir)

.PHONY: thrift-gen-$(SUBDIR)
thrift-gen-$(SUBDIR): install-thrift-bin install-license-bin
	@echo Generating thrift files
	PACKAGE=$(m3db_package) $(auto_gen) src/$(SUBDIR)/$(thrift_output_dir) src/$(SUBDIR)/$(thrift_rules_dir)

.PHONY: proto-gen-$(SUBDIR)
proto-gen-$(SUBDIR): install-proto-bin install-license-bin
	@echo Generating protobuf files
	PACKAGE=$(m3db_package) $(auto_gen) src/$(SUBDIR)/$(proto_output_dir) src/$(SUBDIR)/$(proto_rules_dir)

.PHONY: all-gen-$(SUBDIR)
# NB(prateek): order matters here, mock-gen needs to be last because we sometimes
# generate mocks for thrift/proto generated code.
all-gen-$(SUBDIR): thrift-gen-$(SUBDIR) proto-gen-$(SUBDIR) mock-gen-$(SUBDIR) genny-all-$(SUBDIR)

.PHONY: metalint-$(SUBDIR)
metalint-$(SUBDIR): install-metalinter install-linter-badtime install-linter-importorder
	@($(metalint_check) src/$(SUBDIR)/$(metalint_config) src/$(SUBDIR)/$(metalint_exclude) src/$(SUBDIR))

.PHONY: test-$(SUBDIR)
test-$(SUBDIR):
	SRC_ROOT=./src/$(SUBDIR) make test-base
	gocov convert $(coverfile) | gocov report

.PHONY: test-xml-$(SUBDIR)
test-xml-$(SUBDIR):
	SRC_ROOT=./src/$(SUBDIR) make test-base-xml

.PHONY: test-html-$(SUBDIR)
test-html-$(SUBDIR):
	SRC_ROOT=./src/$(SUBDIR) make test-base-html

# Note: do not test native pooling since it's experimental/deprecated
.PHONY: test-integration-$(SUBDIR)
test-integration-$(SUBDIR):
	SRC_ROOT=./src/$(SUBDIR) TEST_NATIVE_POOLING=false make test-base-integration

# Usage: make test-single-integration name=<test_name>
.PHONY: test-single-integration-$(SUBDIR)
test-single-integration-$(SUBDIR):
	SRC_ROOT=./src/$(SUBDIR) TEST_NATIVE_POOLING=false make test-base-single-integration name=$(name)

.PHONY: test-ci-unit-$(SUBDIR)
test-ci-unit-$(SUBDIR):
	SRC_ROOT=./src/$(SUBDIR) make test-base
	$(codecov_push) -f $(coverfile) -F $(SUBDIR)

.PHONY: test-ci-big-unit-$(SUBDIR)
test-ci-big-unit-$(SUBDIR):
	SRC_ROOT=./src/$(SUBDIR) make test-big-base
	$(codecov_push) -f $(coverfile) -F $(SUBDIR)

.PHONY: test-ci-integration-$(SUBDIR)
test-ci-integration-$(SUBDIR):
	SRC_ROOT=./src/$(SUBDIR) INTEGRATION_TIMEOUT=4m TEST_NATIVE_POOLING=false TEST_SERIES_CACHE_POLICY=$(cache_policy) make test-base-ci-integration
	$(codecov_push) -f $(coverfile) -F $(SUBDIR)

endef

$(foreach SUBDIR,$(SUBDIRS),$(eval $(SUBDIR_RULES)))

.PHONY: clean
clean:
	@rm -f *.html *.xml *.out *.test

.DEFAULT_GOAL := all

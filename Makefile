SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/.ci/common.mk

SHELL=/bin/bash -o pipefail

auto_gen             := .ci/auto-gen.sh
gopath_prefix        := $(GOPATH)/src
license_dir          := .ci/uber-licence
license_node_modules := $(license_dir)/node_modules
m3db_package         := github.com/m3db/m3db
m3db_package_path    := $(gopath_prefix)/$(m3db_package)
mockgen_package      := github.com/golang/mock/mockgen
retool_bin_path      := $(m3db_package_path)/_tools/bin
retool_package       := github.com/twitchtv/retool
metalint_check       := .ci/metalint.sh
metalint_config      := .metalinter.json
metalint_exclude     := .excludemetalint
mocks_output_dir     := generated/mocks
mocks_rules_dir      := generated/mocks
proto_output_dir     := generated/proto
proto_rules_dir      := generated/proto
assets_output_dir    := generated/assets
assets_rules_dir     := generated/assets
thrift_output_dir    := generated/thrift/rpc
thrift_rules_dir     := generated/thrift
vendor_prefix        := vendor
cache_policy         ?= recently_read

BUILD                := $(abspath ./bin)
GO_BUILD_LDFLAGS_CMD := $(abspath ./.ci/go-build-ldflags.sh) $(m3db_package)
GO_BUILD_LDFLAGS     := $(shell $(GO_BUILD_LDFLAGS_CMD))
LINUX_AMD64_ENV      := GOOS=linux GOARCH=amd64 CGO_ENABLED=0
GO_RELEASER_VERSION  := v0.76.1

SERVICES :=     \
	m3dbnode      \
	m3coordinator \
	m3nsch_server \
	m3nsch_client \

SUBDIRS :=    \
	cmd         \
	dbnode      \
	coordinator \
	m3nsch      \

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
	@[ -d $(license_node_modules) ] || (         \
		git submodule update --init --recursive && \
		cd $(license_dir) && npm install           \
	)

# NB(prateek): cannot use retool for mock-gen, as mock-gen reflection mode requires
# it's full source code be present in the GOPATH at runtime.
.PHONY: install-mockgen
install-mockgen:
	@echo Installing mockgen
	@which mockgen >/dev/null || (                                                     \
		rm -rf $(gopath_prefix)/$(mockgen_package)                                    && \
		mkdir -p $(shell dirname $(gopath_prefix)/$(mockgen_package))                 && \
 		cp -r $(vendor_prefix)/$(mockgen_package) $(gopath_prefix)/$(mockgen_package) && \
		go install $(mockgen_package)                                                    \
	)

.PHONY: install-retool
install-retool:
	@which retool >/dev/null || go get $(retool_package)

.PHONY: install-codegen-tools
install-codegen-tools: install-retool
	@echo "Installing retool dependencies"
	@retool sync >/dev/null 2>/dev/null
	@retool build >/dev/null 2>/dev/null

.PHONY: install-stringer
install-stringer:
		@which stringer > /dev/null || go get golang.org/x/tools/cmd/stringer
		@which stringer > /dev/null || (echo "stringer install failed" && exit 1)

.PHONY: install-goreleaser
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
	docker run --rm hello-world >/dev/null
	docker build -t m3db-docs -f scripts/docs.Dockerfile docs

.PHONY: docs-build
docs-build: docs-container
	docker run -v $(PWD):/m3db --rm m3db-docs "mkdocs build -e docs/theme -t material"

.PHONY: docs-serve
docs-serve: docs-container
	docker run -v $(PWD):/m3db -p 8000:8000 -it --rm m3db-docs "mkdocs serve -e docs/theme -t material -a 0.0.0.0:8000"

.PHONY: docs-deploy
docs-deploy: docs-container
	docker run -v $(PWD):/m3db --rm m3db-docs "mkdocs build -e docs/theme -t material && mkdocs gh-deploy --dirty"

.PHONY: docker-integration-test
docker-integration-test:
	@echo "Running Docker integration test"
	@./scripts/integration-tests/docker-integration-test.sh
	@cd scripts/integration-tests/prometheus/ && ./prometheus-integration-test.sh

SUBDIR_TARGETS = mock-gen thrift-gen proto-gen asset-gen all-gen metalint test \
				 test-xml test-ci-unit test-ci-integration

define TARGET_RULE

.PHONY: $(SUBDIR_TARGET)
$(SUBDIR_TARGET): $(patsubst %,$(SUBDIR_TARGET)-%,$(SUBDIRS))

endef

$(foreach SUBDIR_TARGET,$(SUBDIR_TARGETS),$(eval $(TARGET_RULE)))

.PHONY: site-build
site-build:
	@echo "Building site"
	@./scripts/site-build.sh

define SUBDIR_RULES

.PHONY: mock-gen-$(SUBDIR)
mock-gen-$(SUBDIR): install-codegen-tools install-mockgen install-license-bin
	@echo Generating mocks $(SUBDIR)
	@[ ! -d src/$(SUBDIR)/$(mocks_rules_dir) ] || \
		PATH=$(retool_bin_path):$(PATH) PACKAGE=$(m3db_package) $(auto_gen) src/$(SUBDIR)/$(mocks_output_dir) src/$(SUBDIR)/$(mocks_rules_dir)

.PHONY: thrift-gen-$(SUBDIR)
thrift-gen-$(SUBDIR): install-license-bin
	@echo Generating thrift files $(SUBDIR)
	@[ ! -d src/$(SUBDIR)/$(thrift_rules_dir) ] || \
		PATH=$(retool_bin_path):$(PATH) PACKAGE=$(m3db_package) $(auto_gen) src/$(SUBDIR)/$(thrift_output_dir) src/$(SUBDIR)/$(thrift_rules_dir)

.PHONY: proto-gen-$(SUBDIR)
proto-gen-$(SUBDIR): install-license-bin
	@echo Generating protobuf files $(SUBDIR)
	@[ ! -d src/$(SUBDIR)/$(proto_rules_dir) ] || \
		PATH=$(retool_bin_path):$(PATH) PACKAGE=$(m3db_package) $(auto_gen) src/$(SUBDIR)/$(proto_output_dir) src/$(SUBDIR)/$(proto_rules_dir)

.PHONY: asset-gen-$(SUBDIR)
asset-gen-$(SUBDIR): install-codegen-tools install-license-bin
	@echo Generating asset files $(SUBDIR)
	@[ ! -d src/$(SUBDIR)/$(assets_rules_dir) ] || \
		PATH=$(retool_bin_path):$(PATH) PACKAGE=$(m3db_package) $(auto_gen) src/$(SUBDIR)/$(assets_output_dir) src/$(SUBDIR)/$(assets_rules_dir)

.PHONY: genny-gen-$(SUBDIR)
genny-gen-$(SUBDIR):
	@echo Generating genny files $(SUBDIR)
	@[ ! -f $(SELF_DIR)/src/$(SUBDIR)/generated-source-files.mk ] || \
		PATH=$(retool_bin_path):$(PATH) make -f $(SELF_DIR)/src/$(SUBDIR)/generated-source-files.mk genny-all-$(SUBDIR)

.PHONY: all-gen-$(SUBDIR)
# NB(prateek): order matters here, mock-gen needs to be last because we sometimes
# generate mocks for thrift/proto generated code.
all-gen-$(SUBDIR): thrift-gen-$(SUBDIR) proto-gen-$(SUBDIR) asset-gen-$(SUBDIR) genny-gen-$(SUBDIR) mock-gen-$(SUBDIR)

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

GEN_TARGETS := \
	mock-gen     \
	proto-gen    \
	thrift-gen   \
	asset-gen    \
	genny-gen    \
	all-gen      \

define GEN_TARGET_RULE
.PHONY: $(GENTARGET)
$(GENTARGET): $(foreach SUBDIR,$(SUBDIRS),$(GENTARGET)-$(SUBDIR))
endef

$(foreach GENTARGET,$(GEN_TARGETS),$(eval $(GEN_TARGET_RULE)))

# Tests that all currently generated types match their contents if they were regenerated
.PHONY: test-all-gen
test-all-gen: all-gen
	@test "$(shell git diff --shortstat 2>/dev/null)" = "" || (git diff --no-color && echo "Check git status, there are dirty files" && exit 1)
	@test "$(shell git status --porcelain 2>/dev/null | grep "^??")" = "" || (git status --porcelain && echo "Check git status, there are untracked files" && exit 1)

.PHONY: clean
clean:
	@rm -f *.html *.xml *.out *.test

.DEFAULT_GOAL := all

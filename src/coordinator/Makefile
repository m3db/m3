SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/.ci/common.mk

SHELL=/bin/bash -o pipefail

m3coord_package         := github.com/m3db/m3coordinator
gopath_prefix        := $(GOPATH)/src
vendor_prefix        := vendor
license_dir          := .ci/uber-licence
license_node_modules := $(license_dir)/node_modules
html_report          := coverage.html
test                 := .ci/test-cover.sh
test_one_integration := .ci/test-one-integration.sh
test_ci_integration  := .ci/test-integration.sh
convert-test-data    := .ci/convert-test-data.sh
coverfile            := cover.out
coverage_xml         := coverage.xml
junit_xml            := junit.xml
test_log             := test.log
lint_check           := .ci/lint.sh
metalint_check       := .ci/metalint.sh
metalint_config      := .metalinter.json
metalint_exclude     := .excludemetalint
protoc_go_package    := github.com/golang/protobuf/protoc-gen-go
proto_output_dir     := generated/proto
proto_rules_dir      := generated/proto
auto_gen             := .ci/auto-gen.sh
mockgen_package      := github.com/golang/mock/mockgen
mocks_output_dir     := generated/mocks/mocks
mocks_rules_dir      := generated/mocks

BUILD           := $(abspath ./bin)
LINUX_AMD64_ENV := GOOS=linux GOARCH=amd64 CGO_ENABLED=0
VENDOR_ENV      := GO15VENDOREXPERIMENT=1

SERVICES := \
	m3coordinator

TOOLS :=         \

.PHONY: setup
setup:
	mkdir -p $(BUILD)

define SERVICE_RULES

.PHONY: $(SERVICE)
$(SERVICE): setup
	@echo Building $(SERVICE)
	$(VENDOR_ENV) go build -o $(BUILD)/$(SERVICE) ./services/$(SERVICE)/main/.

.PHONY: $(SERVICE)-linux-amd64
$(SERVICE)-linux-amd64:
	$(LINUX_AMD64_ENV) make $(SERVICE)

endef

define TOOL_RULES

.PHONY: $(TOOL)
$(TOOL): setup
	@echo Building $(TOOL)
	$(VENDOR_ENV) go build -o $(BUILD)/$(TOOL) ./tools/$(TOOL)/main/.

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
all: metalint test-ci-unit services tools
	@echo Made all successfully

.PHONY: install-license-bin
install-license-bin: install-vendor
	@echo Installing node modules
	git submodule update --init --recursive
	[ -d $(license_node_modules) ] || (cd $(license_dir) && npm install)

.PHONY: install-mockgen
install-mockgen: install-vendor
	@echo Installing mockgen
	rm -rf $(gopath_prefix)/$(mockgen_package) && \
	cp -r $(vendor_prefix)/$(mockgen_package) $(gopath_prefix)/$(mockgen_package) && \
	go install $(mockgen_package)

.PHONY: install-proto-bin
install-proto-bin: install-vendor
	@echo Installing protobuf binaries
	@echo Note: the protobuf compiler v3.0.0 can be downloaded from https://github.com/google/protobuf/releases or built from source at https://github.com/google/protobuf.
	go install $(m3coord_package)/$(vendor_prefix)/$(protoc_go_package)

.PHONY: proto-gen
proto-gen: install-proto-bin install-license-bin
	@echo Generating protobuf files
	PACKAGE=$(m3coord_package) $(auto_gen) $(proto_output_dir) $(proto_rules_dir)

.PHONY: mock-gen
mock-gen: install-mockgen install-license-bin
	@echo Generating mocks
	PACKAGE=$(m3coord_package) $(auto_gen) $(mocks_output_dir) $(mocks_rules_dir)

.PHONY: all-gen
all-gen: proto-gen

.PHONY: lint
lint:
	@which golint > /dev/null || go get -u github.com/golang/lint/golint
	$(VENDOR_ENV) $(lint_check)

.PHONY: metalint
metalint: install-metalinter install-linter-badtime
	@($(metalint_check) $(metalint_config) $(metalint_exclude))

.PHONY: test-internal
test-internal:
	@which go-junit-report > /dev/null || go get -u github.com/sectioneight/go-junit-report
	@$(VENDOR_ENV) $(test) $(coverfile) | tee $(test_log)

# Note: do not test native pooling since it's experimental/deprecated
.PHONY: test-integration
test-integration:
	TEST_NATIVE_POOLING=false make test-base-integration

.PHONY: test
test: test-base
	# coverfile defined in common.mk
	gocov convert $(coverfile) | gocov report

.PHONY: test-xml
test-xml: test-base-xml

.PHONY: test-html
test-html: test-base-html

.PHONY: test-ci-unit
test-ci-unit: test-base
	$(codecov_push) -f $(coverfile) -F unittests

.PHONY: test-ci-integration
test-ci-integration:
	INTEGRATION_TIMEOUT=4m TEST_NATIVE_POOLING=false TEST_SERIES_CACHE_POLICY=$(cache_policy) make test-base-ci-integration
	$(codecov_push) -f $(coverfile) -F integration

# run as: make test-one-integration test=<test_name>
.PHONY: test-one-integration
test-one-integration:
	@$(VENDOR_ENV) TEST_NATIVE_POOLING=false $(test_one_integration) $(test)

.PHONY: clean
clean:
	@rm -f *.html *.xml *.out *.test

.DEFAULT_GOAL := all

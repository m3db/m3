SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/.ci/common.mk

SHELL=/bin/bash -o pipefail

m3db_package         := github.com/m3db/m3coordinator
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
auto_gen             := .ci/auto-gen.sh
mockgen_package      := github.com/golang/mock/mockgen
mocks_output_dir     := generated/mocks/mocks
mocks_rules_dir      := generated/mocks
thrift_gen_package   := github.com/uber/tchannel-go
thrift_output_dir    := generated/thrift/rpc
thrift_rules_dir     := generated/thrift

BUILD           := $(abspath ./bin)
LINUX_AMD64_ENV := GOOS=linux GOARCH=amd64 CGO_ENABLED=0
VENDOR_ENV      := GO15VENDOREXPERIMENT=1

SERVICES := \
	m3coordinator

TOOLS :=         \
	read_ids       \
	read_index_ids \
	clone_fileset  \
	dtest          \

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
all: lint metalint test-ci-unit test-ci-integration services tools
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

.PHONY: install-thrift-bin
install-thrift-bin: install-vendor install-glide
	@echo Installing thrift binaries
	@echo Note: the thrift binary should be installed from https://github.com/apache/thrift at commit 9b954e6a469fef18682314458e6fc4af2dd84add.
	go get $(thrift_gen_package) && cd $(GOPATH)/src/$(thrift_gen_package) && glide install
	go install $(thrift_gen_package)/thrift/thrift-gen

.PHONY: mock-gen
mock-gen: install-mockgen install-license-bin
	@echo Generating mocks
	PACKAGE=$(m3db_package) $(auto_gen) $(mocks_output_dir) $(mocks_rules_dir)

.PHONY: thrift-gen
thrift-gen: install-thrift-bin install-license-bin
	@echo Generating thrift files
	PACKAGE=$(m3db_package) $(auto_gen) $(thrift_output_dir) $(thrift_rules_dir)

.PHONY: all-gen
all-gen: mock-gen thrift-gen

.PHONY: lint
lint:
	@which golint > /dev/null || go get -u github.com/golang/lint/golint
	$(VENDOR_ENV) $(lint_check)

.PHONY: metalint
metalint: install-metalinter
	@($(metalint_check) $(metalint_config) $(metalint_exclude) \
		&& echo "metalinted successfully!") || (echo "metalinter failed" && exit 1)

.PHONY: test-internal
test-internal:
	@which go-junit-report > /dev/null || go get -u github.com/sectioneight/go-junit-report
	@$(VENDOR_ENV) $(test) $(coverfile) | tee $(test_log)

# Do not test native pooling for now due to slow travis builds
.PHONY: test-integration
test-integration:
	@$(VENDOR_ENV) TEST_NATIVE_POOLING=false go test -v -tags=integration ./integration

.PHONY: test-xml
test-xml: test-internal
	go-junit-report < $(test_log) > $(junit_xml)
	gocov convert $(coverfile) | gocov-xml > $(coverage_xml)
	@$(convert-test-data) $(coverage_xml)
	@rm $(coverfile) &> /dev/null

.PHONY: test
test: test-internal
	gocov convert $(coverfile) | gocov report

.PHONY: testhtml
testhtml: test-internal
	gocov convert $(coverfile) | gocov-html > $(html_report) && open $(html_report)
	@rm -f $(test_log) &> /dev/null

.PHONY: test-ci-unit
test-ci-unit: test-internal
	@which goveralls > /dev/null || go get -u -f github.com/mattn/goveralls
	goveralls -coverprofile=$(coverfile) -service=travis-ci || echo -e "Coveralls failed"

# Do not test native pooling for now due to slow travis builds
.PHONY: test-ci-integration
test-ci-integration:
	@$(VENDOR_ENV) TEST_NATIVE_POOLING=false $(test_ci_integration)

# run as: make test-one-integration test=<test_name>
.PHONY: test-one-integration
test-one-integration:
	@$(VENDOR_ENV) TEST_NATIVE_POOLING=false $(test_one_integration) $(test)

.PHONY: clean
clean:
	@rm -f *.html *.xml *.out *.test

.DEFAULT_GOAL := all

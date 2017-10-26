SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/.ci/common.mk

SHELL=/bin/bash -o pipefail

auto_gen             := .ci/auto-gen.sh
convert-test-data    := .ci/convert-test-data.sh
coverage_xml         := coverage.xml
coverfile            := cover.out
gopath_prefix        := $(GOPATH)/src
html_report          := coverage.html
junit_xml            := junit.xml
license_dir          := .ci/uber-licence
license_node_modules := $(license_dir)/node_modules
lint_check           := .ci/lint.sh
m3index_package      := github.com/m3db/m3index
metalint_check       := .ci/metalint.sh
metalint_config      := .metalinter.json
metalint_exclude     := .excludemetalint
mockgen_package      := github.com/golang/mock/mockgen
mocks_output_dir     := generated/mocks/mocks
mocks_rules_dir      := generated/mocks
proto_rules_dir      := generated/proto
protoc_go_package    := github.com/golang/protobuf/protoc-gen-go
test                 := .ci/test-cover.sh
test_ci_integration  := .ci/test-integration.sh
test_log             := test.log
test_one_integration := .ci/test-one-integration.sh
vendor_prefix        := vendor

BUILD            := $(abspath ./bin)
GO_BUILD_LDFLAGS := $(shell $(abspath ./.ci/go-build-ldflags.sh) $(m3index_package))
LINUX_AMD64_ENV  := GOOS=linux GOARCH=amd64 CGO_ENABLED=0

# SERVICES := \

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

.PHONY: services services-linux-amd64
services: $(SERVICES)
services-linux-amd64:
	$(LINUX_AMD64_ENV) make services

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

.PHONY: install-proto-bin
install-proto-bin: install-vendor
	@echo Installing protobuf binaries
	@echo Note: the protobuf compiler v3.0.0 can be downloaded from https://github.com/google/protobuf/releases or built from source at https://github.com/google/protobuf.
	go install $(m3db_package)/$(vendor_prefix)/$(protoc_go_package)

.PHONY: mock-gen
mock-gen: install-mockgen install-license-bin
	@echo Generating mocks
	PACKAGE=$(m3db_package) $(auto_gen) $(mocks_output_dir) $(mocks_rules_dir)

.PHONY: proto-gen
proto-gen: install-proto-bin install-license-bin
	@echo Generating protobuf files
	PACKAGE=$(m3db_package) $(auto_gen) $(proto_output_dir) $(proto_rules_dir)

.PHONY: all-gen
all-gen: mock-gen proto-gen

.PHONY: lint
lint:
	@which golint > /dev/null || go get -u github.com/golang/lint/golint
	$(lint_check)

.PHONY: metalint
metalint: install-metalinter
	@($(metalint_check) $(metalint_config) $(metalint_exclude) \
		&& echo "metalinted successfully!") || (echo "metalinter failed" && exit 1)

.PHONY: test-internal
test-internal:
	@which go-junit-report > /dev/null || go get -u github.com/sectioneight/go-junit-report
	$(test) $(coverfile) | tee $(test_log)

# Do not test native pooling for now due to slow travis builds
.PHONY: test-integration
test-integration:
	TEST_NATIVE_POOLING=false go test -v -tags=integration ./integration

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
	INTEGRATION_TIMEOUT=2m TEST_NATIVE_POOLING=false $(test_ci_integration)

# run as: make test-one-integration test=<test_name>
.PHONY: test-one-integration
test-one-integration:
	TEST_NATIVE_POOLING=false $(test_one_integration) $(test)

.PHONY: clean
clean:
	@rm -f *.html *.xml *.out *.test

.DEFAULT_GOAL := all

SHELL=/bin/bash -o pipefail

html_report := coverage.html
test := .ci/test-cover.sh
convert-test-data := .ci/convert-test-data.sh
coverfile := cover.out
coverage_xml := coverage.xml
junit_xml := junit.xml
test_log := test.log
lint_check := .ci/lint.sh
m3db_package := github.com/m3db/m3db
gopath_prefix := $(GOPATH)/src
vendor_prefix := vendor
license_dir := $(vendor_prefix)/github.com/uber/uber-licence
license_node_modules := $(license_dir)/node_modules
auto_gen := .ci/auto-gen.sh
mockgen_package := github.com/golang/mock/mockgen
mocks_output_dir := generated/mocks/mocks
mocks_rules_dir := generated/mocks
protoc_go_package := github.com/golang/protobuf/protoc-gen-go
proto_output_dir := generated/proto/schema
proto_rules_dir := generated/proto
thrift_gen_package := github.com/uber/tchannel-go
thrift_output_dir := generated/thrift/rpc
thrift_rules_dir := generated/thrift

BUILD := $(abspath ./bin)
LINUX_AMD64_ENV := GOOS=linux GOARCH=amd64 CGO_ENABLED=0
VENDOR_ENV := GO15VENDOREXPERIMENT=1

SERVICES := \
	m3dbnode
	
TOOLS := 

setup:
	mkdir -p $(BUILD)

define SERVICE_RULES

$(SERVICE): setup
	@echo Building $(SERVICE)
	$(VENDOR_ENV) go build -o $(BUILD)/$(SERVICE) ./services/$(SERVICE)/main/.

$(SERVICE)-linux-amd64:
	$(LINUX_AMD64_ENV) make $(SERVICE)

endef

define TOOL_RULES

$(TOOL): setup
	@echo Building $(TOOL)
	$(VENDOR_ENV) go build -o $(BUILD)/$(TOOL) ./tools/$(TOOL)/main/.

$(TOOL)-linux-amd64:
	$(LINUX_AMD64_ENV) make $(TOOL)

endef

services: $(SERVICES)
services-linux-amd64:
	$(LINUX_AMD64_ENV) make services

tools: $(TOOLS)
tools-linux-amd64:
	$(LINUX_AMD64_ENV) make tools

$(foreach SERVICE,$(SERVICES),$(eval $(SERVICE_RULES)))
$(foreach TOOL,$(TOOLS),$(eval $(TOOL_RULES)))

install-vendor: .gitmodules
	@echo Updating submodules
	git submodule update --init --recursive

install-license-bin: install-vendor
	@echo Installing node modules
	[ -d $(license_node_modules) ] || (cd $(license_dir) && npm install)

install-mockgen: install-vendor
	@echo Installing mockgen
	rm -rf $(gopath_prefix)/$(mockgen_package) && \
	cp -r $(vendor_prefix)/$(mockgen_package) $(gopath_prefix)/$(mockgen_package) && \
	go install $(mockgen_package)

install-proto-bin: install-vendor
	@echo Installing protobuf binaries
	@echo Note: the protobuf compiler v3.0.0 can be downloaded from https://github.com/google/protobuf/releases or built from source at https://github.com/google/protobuf.
	go install $(m3db_package)/$(vendor_prefix)/$(protoc_go_package)

install-glide:
	@which glide > /dev/null || go get -u github.com/Masterminds/glide

install-thrift-bin: install-vendor install-glide
	@echo Installing thrift binaries
	@echo Note: the thrift binary should be installed from https://github.com/apache/thrift at commit 9b954e6a469fef18682314458e6fc4af2dd84add.
	go get $(thrift_gen_package) && cd $(GOPATH)/src/$(thrift_gen_package) && glide install
	go install $(thrift_gen_package)/thrift/thrift-gen

# mock-gen depends on thrift-gen because one of mocks generated is for the tchannel rpc interfaces
mock-gen: install-mockgen install-license-bin thrift-gen
	@echo Generating mocks
	$(auto_gen) $(mocks_output_dir) $(mocks_rules_dir)

proto-gen: install-proto-bin install-license-bin
	@echo Generating protobuf files
	$(auto_gen) $(proto_output_dir) $(proto_rules_dir)

thrift-gen: install-thrift-bin install-license-bin
	@echo Generating thrift files
	$(auto_gen) $(thrift_output_dir) $(thrift_rules_dir)

all-gen: mock-gen proto-gen thrift-gen

lint:
	@which golint > /dev/null || go get -u github.com/golang/lint/golint
	$(VENDOR_ENV) $(lint_check)

test-internal:
	@which go-junit-report > /dev/null || go get -u github.com/sectioneight/go-junit-report
	@$(VENDOR_ENV) $(test) $(coverfile) | tee $(test_log)

test-integration:
	@$(VENDOR_ENV) go test -v -tags=integration ./integration

test-xml: test-internal
	go-junit-report < $(test_log) > $(junit_xml)
	gocov convert $(coverfile) | gocov-xml > $(coverage_xml)
	@$(convert-test-data) $(coverage_xml)
	@rm $(coverfile) &> /dev/null

test: test-internal
	gocov convert $(coverfile) | gocov report

testhtml: test-internal
	gocov convert $(coverfile) | gocov-html > $(html_report) && open $(html_report)
	@rm -f $(test_log) &> /dev/null

install-ci: 
	make install-vendor

test-ci-unit: test-internal
	@which goveralls > /dev/null || go get -u -f github.com/mattn/goveralls
	goveralls -coverprofile=$(coverfile) -service=travis-ci || echo -e "\x1b[31mCoveralls failed\x1b[m"

clean:
	@rm -f *.html *.xml *.out *.test

.DEFAULT_GOAL := test
.PHONY: test test-xml test-internal testhtml clean

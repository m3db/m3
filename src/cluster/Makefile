SHELL=/bin/bash -o pipefail

html_report := coverage.html
test := .ci/test-cover.sh
convert-test-data := .ci/convert-test-data.sh
coverfile := cover.out
coverage_xml := coverage.xml
junit_xml := junit.xml
test_log := test.log
test_target := .
lint_check := .ci/lint.sh
package_root := github.com/m3db/m3cluster
gopath_prefix := $(GOPATH)/src
vendor_prefix := vendor
mockgen_package := github.com/golang/mock/mockgen
mocks_output_dir := generated/mocks/mocks
mocks_rules_dir := generated/mocks
protoc_go_package := github.com/golang/protobuf/protoc-gen-go
proto_output_dir := generated/proto
proto_rules_dir := generated/proto
auto_gen := .ci/auto-gen.sh
license_dir := $(vendor_prefix)/github.com/uber/uber-licence
license_node_modules := $(license_dir)/node_modules

BUILD := $(abspath ./bin)
LINUX_AMD64_ENV := GOOS=linux GOARCH=amd64 CGO_ENABLED=0
VENDOR_ENV := GO15VENDOREXPERIMENT=1

setup:
	mkdir -p $(BUILD)

lint:
	@which golint > /dev/null || go get -u github.com/golang/lint/golint
	$(VENDOR_ENV) $(lint_check)

test-internal:
	@which go-junit-report > /dev/null || go get -u github.com/sectioneight/go-junit-report
	@$(VENDOR_ENV) $(test) $(test_target) $(coverfile) | tee $(test_log)

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

test-ci-unit: test-internal
	@which goveralls > /dev/null || go get -u -f github.com/mattn/goveralls
	goveralls -coverprofile=$(coverfile) -service=travis-ci || echo -e "\x1b[31mCoveralls failed\x1b[m"

install-vendor: .gitmodules
	@echo Updating submodules
	git submodule update --init --recursive

install-mockgen: install-vendor
	@echo Installing mockgen
	glide install

install-license-bin: install-vendor
	@echo Installing node modules
	[ -d $(license_node_modules) ] || (cd $(license_dir) && npm install)

install-proto-bin: install-vendor 
	@echo Installing protobuf binaries
	@echo Note: the protobuf compiler v3.0.0 can be downloaded from https://github.com/google/protobuf/releases or built from source at https://github.com/google/protobuf.
	go install $(package_root)/$(vendor_prefix)/$(protoc_go_package)

mock-gen: install-mockgen install-license-bin
	@echo Generating mocks
	$(auto_gen) $(mocks_output_dir) $(mocks_rules_dir)

proto-gen: install-proto-bin install-license-bin
	@echo Generating protobuf files
	$(auto_gen) $(proto_output_dir) $(proto_rules_dir)

all-gen: proto-gen mock-gen

clean:
	@rm -f *.html *.xml *.out *.test

.DEFAULT_GOAL := test
.PHONY: test test-xml test-internal testhtml clean

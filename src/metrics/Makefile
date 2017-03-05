SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/.ci/common.mk

SHELL=/bin/bash -o pipefail

html_report := coverage.html
test := .ci/test-cover.sh
convert-test-data := .ci/convert-test-data.sh
coverfile := cover.out
coverage_xml := coverage.xml
junit_xml := junit.xml
test_log := test.log
lint_check := .ci/lint.sh
gopath_prefix := $(GOPATH)/src
vendor_prefix := vendor
m3metrics_package := github.com/m3db/m3metrics
license_dir := .ci/uber-licence
license_node_modules := $(license_dir)/node_modules
auto_gen := .ci/auto-gen.sh
protoc_go_package := github.com/golang/protobuf/protoc-gen-go
proto_output_dir := generated/proto/schema
proto_rules_dir := generated/proto

BUILD := $(abspath ./bin)
LINUX_AMD64_ENV := GOOS=linux GOARCH=amd64 CGO_ENABLED=0

setup:
	mkdir -p $(BUILD)

install-license-bin: install-vendor
	@echo Installing node modules
	git submodule update --init --recursive
	[ -d $(license_node_modules) ] || (cd $(license_dir) && npm install)

install-proto-bin: install-vendor
	@echo Installing protobuf binaries
	@echo Note: the protobuf compiler v3.0.0 can be downloaded from https://github.com/google/protobuf/releases or built from source at https://github.com/google/protobuf.
	go install $(m3metrics_package)/$(vendor_prefix)/$(protoc_go_package)

proto-gen: install-proto-bin install-license-bin
	@echo Generating protobuf files
	PACKAGE=$(m3metrics_package) $(auto_gen) $(proto_output_dir) $(proto_rules_dir)

all-gen: proto-gen

lint:
	@which golint > /dev/null || go get -u github.com/golang/lint/golint
	$(lint_check)

test-internal:
	@which go-junit-report > /dev/null || go get -u github.com/sectioneight/go-junit-report
	$(test) $(coverfile) | tee $(test_log)

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

clean:
	@rm -f *.html *.xml *.out *.test

.DEFAULT_GOAL := test
.PHONY: test test-xml test-internal testhtml clean

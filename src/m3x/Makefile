SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/.ci/common.mk
include $(SELF_DIR)/generated-source-files.mk

SHELL=/bin/bash -o pipefail

html_report           := coverage.html
test                  := .ci/test-cover.sh
convert-test-data     := .ci/convert-test-data.sh
coverage_xml          := coverage.xml
junit_xml             := junit.xml
test_log              := test.log
metalint_check        := .ci/metalint.sh
metalint_config       := .metalinter.json
metalint_exclude      := .excludemetalint
license_dir           := .ci/uber-licence
license_node_modules  := $(license_dir)/node_modules
gopath_prefix         := $(GOPATH)/src
vendor_prefix         := vendor
auto_gen              := .ci/auto-gen.sh
m3x_package           := github.com/m3db/m3x
mockgen_package       := github.com/golang/mock/mockgen
mocks_output_dir      := generated/mocks/mocks
mocks_rules_dir       := generated/mocks

BUILD           := $(abspath ./bin)
LINUX_AMD64_ENV := GOOS=linux GOARCH=amd64 CGO_ENABLED=0

.PHONY: setup
setup:
	mkdir -p $(BUILD)

.PHONY: metalint
metalint: install-metalinter install-linter-badtime
	@($(metalint_check) $(metalint_config) $(metalint_exclude) && echo "metalinted successfully!") || (echo "metalinter failed" && exit 1)

.PHONY: test-internal
test-internal:
	@which go-junit-report > /dev/null || go get -u github.com/sectioneight/go-junit-report
	$(test) $(coverfile) | tee $(test_log)

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
test-ci-unit: test-base
	$(codecov_push) -f $(coverfile)

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

.PHONY: mock-gen
mock-gen: install-mockgen install-license-bin install-util-mockclean
	@echo Generating mocks
	PACKAGE=$(m3x_package) $(auto_gen) $(mocks_output_dir) $(mocks_rules_dir)

.PHONY: all-gen
all-gen: mock-gen genny-all

.PHONY: clean
clean:
	@rm -f *.html *.xml *.out *.test

.PHONY: all
all: metalint test-ci-unit test-genny-all
	@echo Made all successfully

.DEFAULT_GOAL := all

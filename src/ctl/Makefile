SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/.ci/common.mk

SHELL=/bin/bash -o pipefail

html_report         := coverage.html
test                := .ci/test-cover.sh
test_ci_integration := .ci/test-integration.sh
convert-test-data   := .ci/convert-test-data.sh
coverfile           := cover.out
coverage_xml        := coverage.xml
junit_xml           := junit.xml
test_log            := test.log
lint_check          := .ci/lint.sh
metalint_check      := .ci/metalint.sh
metalint_config     := .metalinter.json
metalint_exclude    := .excludemetalint
m3ctl_package       := github.com/m3db/m3ctl
gopath_prefix       := $(GOPATH)/src
vendor_prefix       := vendor

BUILD           := $(abspath ./bin)
LINUX_AMD64_ENV := GOOS=linux GOARCH=amd64 CGO_ENABLED=0

SERVICES := \
	m3ctl

.PHONY: setup
setup:
	mkdir -p $(BUILD)

define SERVICE_RULES

$(SERVICE): setup
	@echo Building $(SERVICE)
	$(VENDOR_ENV) go build -o $(BUILD)/$(SERVICE) ./services/$(SERVICE)/main/.

$(SERVICE)-linux-amd64:
	$(LINUX_AMD64_ENV) make $(SERVICE)

endef

services: $(SERVICES)
services-linux-amd64:
	$(LINUX_AMD64_ENV) make services

$(foreach SERVICE,$(SERVICES),$(eval $(SERVICE_RULES)))

.PHONY: lint
lint:
	@which golint > /dev/null || go get -u github.com/golang/lint/golint
	$(lint_check)

.PHONY: metalint
metalint: install-metalinter
	@($(metalint_check) $(metalint_config) $(metalint_exclude) && echo "metalinted successfully!") || (echo "metalinter failed" && exit 1)

.PHONY: test-internal
test-internal:
	@which go-junit-report > /dev/null || go get -u github.com/sectioneight/go-junit-report
	$(test) $(coverfile) | tee $(test_log)

.PHONY: test-integration
test-integration:
	go test -v -tags=integration ./integration

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
	goveralls -coverprofile=$(coverfile) -service=travis-ci || echo -e "\x1b[31mCoveralls failed\x1b[m"

.PHONY: test-ci-integration
test-ci-integration:
	$(test_ci_integration)

.PHONY: clean
clean:
	@rm -f *.html *.xml *.out *.test

.PHONY: all
all: lint metalint test-ci-unit services
	@echo Made all successfully

.DEFAULT_GOAL := all

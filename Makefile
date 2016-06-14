SHELL=/bin/bash -o pipefail

html_report := coverage.html
test := .jenkins/test-cover.sh
convert-test-data := .jenkins/convert-test-data.sh
coverfile := cover.out
coverage_xml := coverage.xml
junit_xml := junit.xml
test_log := test.log
test_target := .

BUILD := $(abspath ./bin)
LINUX_AMD64_ENV := GOOS=linux GOARCH=amd64 CGO_ENABLED=0
VENDOR_ENV := GO15VENDOREXPERIMENT=1

SERVICES := \
	m3dbnode
	
TOOLS := \
	benchmarkencoding \
	benchmarkbootstrap \
	benchmarkoverhead \
	benchmarkfsconvertfstofs2 \
	benchmarkfsreadfs2

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

test-internal:
	@which go-junit-report > /dev/null || go get -u github.com/sectioneight/go-junit-report
	@$(VENDOR_ENV) $(test) $(test_target) $(coverfile) | tee $(test_log)

test-xml: test-internal
	go-junit-report < $(test_log) > $(junit_xml)
	gocov convert $(coverfile) | gocov-xml > $(coverage_xml)
	@$(convert-test-data) $(coverage_xml)
	@rm $(coverfile) &> /dev/null

test: test-internal
	@$(test)
	gocov convert $(coverfile) | gocov report

testhtml: test-internal
	gocov convert $(coverfile) | gocov-html > $(html_report) && open $(html_report)
	@rm -f $(test_log) &> /dev/null

clean:
	@rm -f *.html *.xml *.out *.test

.DEFAULT_GOAL := test
.PHONY: test test-xml test-internal testhtml clean

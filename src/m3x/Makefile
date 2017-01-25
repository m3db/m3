SHELL=/bin/bash -o pipefail

html_report := coverage.html
test := .ci/test-cover.sh
convert-test-data := .ci/convert-test-data.sh
coverfile := cover.out
coverage_xml := coverage.xml
junit_xml := junit.xml
test_log := test.log
lint_check := .ci/lint.sh

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
	@$(VENDOR_ENV) $(test) $(coverfile) | tee $(test_log)

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

SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/.ci/common.mk

SHELL=/bin/bash -o pipefail

html_report          := coverage.html
test                 := .ci/test-cover.sh
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
m3ctl_package        := github.com/m3db/m3ctl
gopath_prefix        := $(GOPATH)/src
vendor_prefix        := vendor
mocks_output_dir     := generated/mocks/mocks
package_root         := github.com/m3db/m3ctl
mocks_rules_dir      := generated/mocks
ui_codegen_dir       := generated/ui
auto_gen             := .ci/auto-gen.sh
license_node_modules := $(license_dir)/node_modules
license_dir          := .ci/uber-licence
retool_bin_path      := _tools/bin
retool_package       := github.com/twitchtv/retool
node_version         := v6

BUILD           := $(abspath ./bin)
LINUX_AMD64_ENV := GOOS=linux GOARCH=amd64 CGO_ENABLED=0

SERVICES := \
	r2ctl

.PHONY: setup
setup:
	mkdir -p $(BUILD)

define SERVICE_RULES

$(SERVICE): setup
ifeq ($(SERVICE),r2ctl)
	@echo "Building $(SERVICE) dependencies"
	make build-ui-statik-packages
endif
	@echo Building $(SERVICE)
	$(VENDOR_ENV) go build -o $(BUILD)/$(SERVICE) ./services/$(SERVICE)/main/.

$(SERVICE)-linux-amd64:
	$(LINUX_AMD64_ENV) make $(SERVICE)

endef

services: $(SERVICES)
services-linux-amd64:
	$(LINUX_AMD64_ENV) make services

cmd-using-node-version:
ifneq ($(shell brew --prefix nvm 2>/dev/null),)
	@echo "Using nvm from brew to select node version $(node_version)"
	source $(shell brew --prefix nvm)/nvm.sh && nvm use $(node_version) && bash -c "$(node_cmd)"
else ifneq ($(shell type nvm 2>/dev/null),)
	@echo "Using nvm to select node version $(node_version)"
	nvm use $(node_version) && bash -c "$(node_cmd)"
else
	@echo "Not using nvm, using node version $(shell node --version)"
	bash -c "$(node_cmd)"
endif

build-ui:
ifeq ($(shell ls ./ui/build 2>/dev/null),)
	# Need to use subshell output of set-node-version as cannot
	# set side-effects of nvm to later commands
	@echo "Building UI components, if npm install or build fails try: npm cache clean"
	make cmd-using-node-version node_cmd="cd ui && npm install && npm run build"
else
	@echo "Skip building UI components, already built, to rebuild first make clean"
endif
	# Move public assets into public subdirectory so that it can
	# be included in the single statik package built from ./ui/build
	rm -rf ./ui/build/public
	cp -r ./public ./ui/build/public

build-ui-statik-packages: build-ui install-tools
	mkdir -p $(ui_codegen_dir)
	$(retool_bin_path)/statik -src ./ui/build -dest $(ui_codegen_dir) -p statik

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
	@rm -f *.html *.xml *.out *.test $(BUILD)/*
	@rm -rf ./ui/build

.PHONY: all
all: lint metalint test-ci-unit services
	@echo Made all successfully

.PHONY: install-licence-bin
install-license-bin: install-vendor
	@echo Installing node modules
	git submodule update --init --recursive
	[ -d $(license_node_modules) ] || (cd $(license_dir) && npm install)

.PHONY: install-mockgen
install-mockgen: install-vendor
	@echo Installing mockgen
	glide install

.PHONY: install-retool
install-retool:
	@which retool >/dev/null || go get $(retool_package)

.PHONY: install-tools
install-tools: install-retool
	@echo "Installing tools"
	@retool sync >/dev/null 2>/dev/null
	@retool build >/dev/null 2>/dev/null

.PHONY: mock-gen
mock-gen: install-mockgen install-license-bin install-util-mockclean
	@echo Generating mocks
	PACKAGE=$(package_root) $(auto_gen) $(mocks_output_dir) $(mocks_rules_dir)

.PHONY: mock-gen-no-deps
mock-gen-no-deps:
	@echo Generating mocks
	PACKAGE=$(package_root) $(auto_gen) $(mocks_output_dir) $(mocks_rules_dir)

.DEFAULT_GOAL := all

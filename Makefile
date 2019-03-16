SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))

# Grab necessary submodules, in case the repo was cloned without --recursive
$(SELF_DIR)/.ci/common.mk:
	git submodule update --init --recursive

include $(SELF_DIR)/.ci/common.mk

SHELL=/bin/bash -o pipefail

auto_gen             := scripts/auto-gen.sh
process_coverfile    := scripts/process-cover.sh
gopath_prefix        := $(GOPATH)/src
gopath_bin_path      := $(GOPATH)/bin
m3_package           := github.com/m3db/m3
m3_package_path      := $(gopath_prefix)/$(m3_package)
mockgen_package      := github.com/golang/mock/mockgen
retool_bin_path      := $(m3_package_path)/_tools/bin
retool_src_prefix    := $(m3_package_path)/_tools/src
retool_package       := github.com/twitchtv/retool
metalint_check       := .ci/metalint.sh
metalint_config      := .metalinter.json
metalint_exclude     := .excludemetalint
mocks_output_dir     := generated/mocks
mocks_rules_dir      := generated/mocks
proto_output_dir     := generated/proto
proto_rules_dir      := generated/proto
assets_output_dir    := generated/assets
assets_rules_dir     := generated/assets
thrift_output_dir    := generated/thrift/rpc
thrift_rules_dir     := generated/thrift
vendor_prefix        := vendor
cache_policy         ?= recently_read

BUILD                     := $(abspath ./bin)
VENDOR                    := $(m3_package_path)/$(vendor_prefix)
GO_BUILD_LDFLAGS_CMD      := $(abspath ./.ci/go-build-ldflags.sh) $(m3_package)
GO_BUILD_LDFLAGS          := $(shell $(GO_BUILD_LDFLAGS_CMD))
GO_BUILD_COMMON_ENV       := CGO_ENABLED=0
LINUX_AMD64_ENV           := GOOS=linux GOARCH=amd64 $(GO_BUILD_COMMON_ENV)
GO_RELEASER_DOCKER_IMAGE  := goreleaser/goreleaser:v0.93
GO_RELEASER_WORKING_DIR   := /go/src/github.com/m3db/m3
GOMETALINT_VERSION        := v2.0.5

# LD Flags
GIT_REVISION              := $(shell git rev-parse --short HEAD)
GIT_BRANCH                := $(shell git rev-parse --abbrev-ref HEAD)
GIT_VERSION               := $(shell git describe --tags --abbrev=0 2>/dev/null || echo unknown)
BUILD_DATE                := $(shell date '+%F-%T') # outputs something in this format 2017-08-21-18:58:45
BUILD_TS_UNIX             := $(shell date '+%s') # second since epoch
BASE_PACKAGE              := ${m3_package}/vendor/github.com/m3db/m3x/instrument

export NPROC := 2 # Maximum package concurrency for unit tests.

SERVICES :=     \
	m3dbnode      \
	m3coordinator \
	m3aggregator  \
	m3query       \
	m3collector   \
	m3ctl         \
	m3em_agent    \
	m3nsch_server \
	m3nsch_client \

SUBDIRS :=    \
	x           \
	cluster     \
	msg         \
	metrics     \
	cmd         \
	collector   \
	dbnode      \
	query       \
	m3em        \
	m3nsch      \
	m3ninx      \
	aggregator  \
	ctl         \
	kube        \

TOOLS :=               \
	read_ids             \
	read_index_ids       \
	read_data_files      \
	read_index_files     \
	clone_fileset        \
	dtest                \
	verify_commitlogs    \
	verify_index_files   \
	carbon_load

.PHONY: setup
setup:
	mkdir -p $(BUILD)

define SERVICE_RULES

.PHONY: $(SERVICE)
$(SERVICE): setup
ifeq ($(SERVICE),m3ctl)
	@echo "Building $(SERVICE) dependencies"
	make build-ui-ctl-statik-gen
endif
	@echo Building $(SERVICE)
	[ -d $(VENDOR) ] || make install-vendor
	$(GO_BUILD_COMMON_ENV) go build -ldflags '$(GO_BUILD_LDFLAGS)' -o $(BUILD)/$(SERVICE) ./src/cmd/services/$(SERVICE)/main/.

.PHONY: $(SERVICE)-linux-amd64
$(SERVICE)-linux-amd64:
	$(LINUX_AMD64_ENV) make $(SERVICE)

endef

$(foreach SERVICE,$(SERVICES),$(eval $(SERVICE_RULES)))

define TOOL_RULES

.PHONY: $(TOOL)
$(TOOL): setup
	@echo Building $(TOOL)
	go build -o $(BUILD)/$(TOOL) ./src/cmd/tools/$(TOOL)/main/.

.PHONY: $(TOOL)-linux-amd64
$(TOOL)-linux-amd64:
	$(LINUX_AMD64_ENV) make $(TOOL)

endef

$(foreach TOOL,$(TOOLS),$(eval $(TOOL_RULES)))

.PHONY: services services-linux-amd64
services: $(SERVICES)
services-linux-amd64:
	$(LINUX_AMD64_ENV) make services

.PHONY: tools tools-linux-amd64
tools: $(TOOLS)
tools-linux-amd64:
	$(LINUX_AMD64_ENV) make tools

.PHONY: all
all: metalint test-ci-unit test-ci-integration services tools
	@echo Made all successfully

.PHONY: install-retool
install-retool:
	@which retool >/dev/null || go get $(retool_package)

.PHONY: install-tools
install-tools: install-retool
	@echo "Installing retool dependencies"
	retool sync
	retool build

	@# NB(r): to ensure correct version of mock-gen is present we match the version
	@# of the retool installed mockgen, and if not a match in binary contents, then
	@# we explicitly install at the version we desire.
	@# We cannot solely use the retool binary as mock-gen requires its full source
	@# code to be present in the GOPATH at runtime.
	@echo "Installing mockgen"
	$(eval curr_mockgen_md5=`cat $(gopath_bin_path)/mockgen | go run $(m3_package_path)/scripts/md5/md5.go`)
	$(eval retool_mockgen_md5=`cat $(retool_bin_path)/mockgen | go run $(m3_package_path)/scripts/md5/md5.go`)
	@test "$(curr_mockgen_md5)" = "$(retool_mockgen_md5)" && echo "Mockgen already up to date" || ( \
		echo "Installing mockgen from Retool directory"                                            && \
		rm -rf $(gopath_prefix)/$(mockgen_package)                                                 && \
		mkdir -p $(shell dirname $(gopath_prefix)/$(mockgen_package))                              && \
 		cp -r $(retool_src_prefix)/$(mockgen_package) $(gopath_prefix)/$(mockgen_package)          && \
		(rm $(gopath_bin_path)/mockgen || echo "No installed mockgen" > /dev/null)                 && \
		cp $(retool_bin_path)/mockgen $(gopath_bin_path)/mockgen                                   && \
		echo "Installed mockgen from Retool directory"                                                \
	)

.PHONY: install-gometalinter
install-gometalinter:
	@mkdir -p $(retool_bin_path)
	./scripts/install-gometalinter.sh -b $(retool_bin_path) -d $(GOMETALINT_VERSION)

.PHONY: check-for-goreleaser-github-token
check-for-goreleaser-github-token:
  ifndef GITHUB_TOKEN
		echo "Usage: make GITHUB_TOKEN=\"<TOKEN>\" release"
		exit 1
  endif

.PHONY: release
release: check-for-goreleaser-github-token
	@echo Releasing new version
	docker run -e "GITHUB_TOKEN=$(GITHUB_TOKEN)" -e "GIT_REVISION=$(GIT_REVISION)" -e "GIT_BRANCH=$(GIT_BRANCH)" -e "GIT_VERSION=$(GIT_VERSION)" -e "BUILD_DATE=$(BUILD_DATE)" -e "BUILD_TS_UNIX=$(BUILD_TS_UNIX)" -e "BASE_PACKAGE=$(BASE_PACKAGE)" -v $(PWD):$(GO_RELEASER_WORKING_DIR) -w $(GO_RELEASER_WORKING_DIR) $(GO_RELEASER_DOCKER_IMAGE) release --rm-dist

.PHONY: release-snapshot
release-snapshot: check-for-goreleaser-github-token
	@echo Creating snapshot release
	docker run -e "GITHUB_TOKEN=$(GITHUB_TOKEN)" -v $(PWD):$(GO_RELEASER_WORKING_DIR) -w $(GO_RELEASER_WORKING_DIR) $(GO_RELEASER_DOCKER_IMAGE) --snapshot --rm-dist

.PHONY: docs-container
docs-container:
	docker run --rm hello-world >/dev/null
	docker build -t m3db-docs -f scripts/docs.Dockerfile docs

.PHONY: docs-build
docs-build: docs-container
	docker run -v $(PWD):/m3db --rm m3db-docs "mkdocs build -e docs/theme -t material"

.PHONY: docs-serve
docs-serve: docs-container
	docker run -v $(PWD):/m3db -p 8000:8000 -it --rm m3db-docs "mkdocs serve -e docs/theme -t material -a 0.0.0.0:8000"

.PHONY: docs-deploy
docs-deploy: docs-container
	docker run -v $(PWD):/m3db --rm -v $(HOME)/.ssh/id_rsa:/root/.ssh/id_rsa:ro -it m3db-docs "mkdocs build -e docs/theme -t material && mkdocs gh-deploy --force --dirty"

.PHONY: docker-integration-test
docker-integration-test:
	@echo "--- Running Docker integration test"
	@./scripts/docker-integration-tests/setup.sh
	@./scripts/docker-integration-tests/simple/test.sh
	@./scripts/docker-integration-tests/prometheus/test.sh
	@./scripts/docker-integration-tests/carbon/test.sh

.PHONY: site-build
site-build:
	@echo "Building site"
	@./scripts/site-build.sh

SUBDIR_TARGETS :=     \
	mock-gen            \
	thrift-gen          \
	proto-gen           \
	asset-gen           \
	genny-gen           \
	license-gen         \
	all-gen             \
	metalint

.PHONY: test-ci-unit
test-ci-unit: test-base
	$(process_coverfile) $(coverfile)

.PHONY: test-ci-big-unit
test-ci-big-unit: test-big-base
	$(process_coverfile) $(coverfile)

.PHONY: test-ci-integration
test-ci-integration:
	INTEGRATION_TIMEOUT=4m TEST_SERIES_CACHE_POLICY=$(cache_policy) make test-base-ci-integration
	$(process_coverfile) $(coverfile)

define SUBDIR_RULES

# We override the rules for `*-gen-kube` to just generate the kube manifest
# bundle.
ifeq ($(SUBDIR), kube)

# Builds the single kube bundle from individual manifest files.
all-gen-kube: install-tools
	@echo "--- Generating kube bundle"
	@./kube/scripts/build_bundle.sh
	find kube -name '*.yaml' -print0 | PATH=$(retool_bin_path):$(PATH) xargs -0 kubeval -v=1.12.0

else

.PHONY: mock-gen-$(SUBDIR)
mock-gen-$(SUBDIR): install-tools
	@echo "--- Generating mocks $(SUBDIR)"
	@[ ! -d src/$(SUBDIR)/$(mocks_rules_dir) ] || \
		PATH=$(retool_bin_path):$(PATH) PACKAGE=$(m3_package) $(auto_gen) src/$(SUBDIR)/$(mocks_output_dir) src/$(SUBDIR)/$(mocks_rules_dir)

.PHONY: thrift-gen-$(SUBDIR)
thrift-gen-$(SUBDIR): install-tools
	@echo "--- Generating thrift files $(SUBDIR)"
	@[ ! -d src/$(SUBDIR)/$(thrift_rules_dir) ] || \
		PATH=$(retool_bin_path):$(PATH) PACKAGE=$(m3_package) $(auto_gen) src/$(SUBDIR)/$(thrift_output_dir) src/$(SUBDIR)/$(thrift_rules_dir)

.PHONY: proto-gen-$(SUBDIR)
proto-gen-$(SUBDIR): install-tools
	@echo "--- Generating protobuf files $(SUBDIR)"
	@[ ! -d src/$(SUBDIR)/$(proto_rules_dir) ] || \
		PATH=$(retool_bin_path):$(PATH) PACKAGE=$(m3_package) $(auto_gen) src/$(SUBDIR)/$(proto_output_dir) src/$(SUBDIR)/$(proto_rules_dir)

.PHONY: asset-gen-$(SUBDIR)
asset-gen-$(SUBDIR): install-tools
	@echo "--- Generating asset files $(SUBDIR)"
	@[ ! -d src/$(SUBDIR)/$(assets_rules_dir) ] || \
		PATH=$(retool_bin_path):$(PATH) PACKAGE=$(m3_package) $(auto_gen) src/$(SUBDIR)/$(assets_output_dir) src/$(SUBDIR)/$(assets_rules_dir)

.PHONY: genny-gen-$(SUBDIR)
genny-gen-$(SUBDIR): install-tools
	@echo "--- Generating genny files $(SUBDIR)"
	@[ ! -f $(SELF_DIR)/src/$(SUBDIR)/generated-source-files.mk ] || \
		PATH=$(retool_bin_path):$(PATH) make -f $(SELF_DIR)/src/$(SUBDIR)/generated-source-files.mk genny-all
	@PATH=$(retool_bin_path):$(PATH) bash -c "source ./scripts/auto-gen-helpers.sh && gen_cleanup_dir '*_gen.go' $(SELF_DIR)/src/$(SUBDIR)/ && gen_cleanup_dir '*_gen_test.go' $(SELF_DIR)/src/$(SUBDIR)/"

.PHONY: license-gen-$(SUBDIR)
license-gen-$(SUBDIR): install-tools
	@echo "--- Updating license in files $(SUBDIR)"
	@find $(SELF_DIR)/src/$(SUBDIR) -name '*.go' | PATH=$(retool_bin_path):$(PATH) xargs -I{} update-license {}

.PHONY: all-gen-$(SUBDIR)
# NB(prateek): order matters here, mock-gen needs to be after proto/thrift because we sometimes
# generate mocks for thrift/proto generated code. Similarly, license-gen needs to be last because
# we make header changes.
all-gen-$(SUBDIR): thrift-gen-$(SUBDIR) proto-gen-$(SUBDIR) asset-gen-$(SUBDIR) genny-gen-$(SUBDIR) mock-gen-$(SUBDIR) license-gen-$(SUBDIR)

.PHONY: test-$(SUBDIR)
test-$(SUBDIR):
	@echo testing $(SUBDIR)
	SRC_ROOT=./src/$(SUBDIR) make test-base
	gocov convert $(coverfile) | gocov report

.PHONY: test-xml-$(SUBDIR)
test-xml-$(SUBDIR):
	@echo test-xml $(SUBDIR)
	SRC_ROOT=./src/$(SUBDIR) make test-base-xml

.PHONY: test-html-$(SUBDIR)
test-html-$(SUBDIR):
	@echo test-html $(SUBDIR)
	SRC_ROOT=./src/$(SUBDIR) make test-base-html

.PHONY: test-integration-$(SUBDIR)
test-integration-$(SUBDIR):
	@echo test-integration $(SUBDIR)
	SRC_ROOT=./src/$(SUBDIR) make test-base-integration

# Usage: make test-single-integration name=<test_name>
.PHONY: test-single-integration-$(SUBDIR)
test-single-integration-$(SUBDIR):
	SRC_ROOT=./src/$(SUBDIR) make test-base-single-integration name=$(name)

.PHONY: test-ci-unit-$(SUBDIR)
test-ci-unit-$(SUBDIR):
	@echo "--- test-ci-unit $(SUBDIR)"
	SRC_ROOT=./src/$(SUBDIR) make test-base
	@echo "--- uploading coverage report"
	$(codecov_push) -f $(coverfile) -F $(SUBDIR)

.PHONY: test-ci-big-unit-$(SUBDIR)
test-ci-big-unit-$(SUBDIR):
	@echo "--- test-ci-big-unit $(SUBDIR)"
	SRC_ROOT=./src/$(SUBDIR) make test-big-base
	@echo "--- uploading coverage report"
	$(codecov_push) -f $(coverfile) -F $(SUBDIR)

.PHONY: test-ci-integration-$(SUBDIR)
test-ci-integration-$(SUBDIR):
	@echo "--- test-ci-integration $(SUBDIR)"
	SRC_ROOT=./src/$(SUBDIR) PANIC_ON_INVARIANT_VIOLATED=false INTEGRATION_TIMEOUT=4m TEST_SERIES_CACHE_POLICY=$(cache_policy) make test-base-ci-integration
	@echo "--- uploading coverage report"
	$(codecov_push) -f $(coverfile) -F $(SUBDIR)

.PHONY: metalint-$(SUBDIR)
metalint-$(SUBDIR): install-gometalinter install-linter-badtime install-linter-importorder
	@echo "--- metalinting $(SUBDIR)"
	@(PATH=$(retool_bin_path):$(PATH) $(metalint_check) \
		$(metalint_config) $(metalint_exclude) src/$(SUBDIR))

endif

endef

# generate targets for each SUBDIR in SUBDIRS based on the rules specified above.
$(foreach SUBDIR,$(SUBDIRS),$(eval $(SUBDIR_RULES)))

define SUBDIR_TARGET_RULE
.PHONY: $(SUBDIR_TARGET)
$(SUBDIR_TARGET): $(foreach SUBDIR,$(SUBDIRS),$(SUBDIR_TARGET)-$(SUBDIR))
endef

# generate targets across SUBDIRS for each SUBDIR_TARGET. i.e. generate rules
# which allow `make all-gen` to invoke `make all-gen-dbnode all-gen-coordinator ...`
# NB: we skip metalint explicity as the default target below requires less invocations
# of metalint and finishes faster.
$(foreach SUBDIR_TARGET, $(filter-out metalint,$(SUBDIR_TARGETS)), $(eval $(SUBDIR_TARGET_RULE)))

.PHONY: build-ui-ctl
build-ui-ctl:
ifeq ($(shell ls ./src/ctl/ui/build 2>/dev/null),)
	# Need to use subshell output of set-node-version as cannot
	# set side-effects of nvm to later commands
	@echo "Building UI components, if npm install or build fails try: npm cache clean"
	make node-yarn-run \
		node_version="6" \
		node_cmd="cd $(m3_package_path)/src/ctl/ui && yarn install && npm run build"
else
	@echo "Skip building UI components, already built, to rebuild first make clean"
endif
	# Move public assets into public subdirectory so that it can
	# be included in the single statik package built from ./ui/build
	rm -rf ./src/ctl/ui/build/public
	cp -r ./src/ctl/public ./src/ctl/ui/build/public

.PHONY: build-ui-ctl-statik-gen
build-ui-ctl-statik-gen: build-ui-ctl-statik license-gen-ctl

.PHONY: build-ui-ctl-statik
build-ui-ctl-statik: build-ui-ctl install-tools
	mkdir -p ./src/ctl/generated/ui
	$(retool_bin_path)/statik -f -src ./src/ctl/ui/build -dest ./src/ctl/generated/ui -p statik

.PHONY: node-yarn-run
node-yarn-run:
	make node-run \
		node_version="$(node_version)" \
		node_cmd="(yarn --version 2>&1 >/dev/null || npm install -g yarn) && $(node_cmd)"

.PHONY: node-run
node-run:
ifneq ($(shell brew --prefix nvm 2>/dev/null),)
	@echo "Using nvm from brew to select node version $(node_version)"
	source $(shell brew --prefix nvm)/nvm.sh && nvm use $(node_version) && bash -c "$(node_cmd)"
else ifneq ($(shell type nvm 2>/dev/null),)
	@echo "Using nvm to select node version $(node_version)"
	nvm use $(node_version) && bash -c "$(node_cmd)"
else
	node --version 2>&1 >/dev/null || \
		(echo "Trying apt install" && which apt-get && (curl -sL https://deb.nodesource.com/setup_$(node_version).x | bash) && apt-get install -y nodejs) || \
		(echo "Trying apk install" && which apk && apk add --update nodejs nodejs-npm) || \
		(echo "No node install or known package manager" && exit 1)
	@echo "Not using nvm, using node version $(shell node --version)"
	bash -c "$(node_cmd)"
endif

.PHONY: metalint
metalint: install-gometalinter install-linter-badtime install-linter-importorder
	@echo "--- metalinting src/"
	@(PATH=$(retool_bin_path):$(PATH) $(metalint_check) \
		$(metalint_config) $(metalint_exclude) src/)

# Tests that all currently generated types match their contents if they were regenerated
.PHONY: test-all-gen
test-all-gen: all-gen
	@test "$(shell git diff --exit-code --shortstat 2>/dev/null)" = "" || (git diff --text --exit-code && echo "Check git status, there are dirty files" && exit 1)
	@test "$(shell git status --exit-code --porcelain 2>/dev/null | grep "^??")" = "" || (git status --exit-code --porcelain && echo "Check git status, there are untracked files" && exit 1)

# Runs a fossa license report
.PHONY: fossa
fossa: install-tools
	PATH=$(retool_bin_path):$(PATH) fossa --option allow-nested-vendor:true --option allow-deep-vendor:true

# Waits for the result of a fossa test and exits success if pass or fail if fails
.PHONY: fossa-test
fossa-test: fossa
	PATH=$(retool_bin_path):$(PATH) fossa test

.PHONY: clean
clean:
	@rm -f *.html *.xml *.out *.test
	@rm -rf $(BUILD)
	@rm -rf $(VENDOR)
	@rm -rf ./src/ctl/ui/build

.DEFAULT_GOAL := all

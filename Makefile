SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))

# Grab necessary submodules, in case the repo was cloned without --recursive
$(SELF_DIR)/.ci/common.mk:
	git submodule update --init --recursive

include $(SELF_DIR)/.ci/common.mk

SHELL=/bin/bash -o pipefail
GOPATH=$(shell eval $$(go env | grep GOPATH) && echo $$GOPATH)

auto_gen             := scripts/auto-gen.sh
process_coverfile    := scripts/process-cover.sh
gopath_prefix        := $(GOPATH)/src
gopath_bin_path      := $(GOPATH)/bin
m3_package           := github.com/m3db/m3
m3_package_path      := $(gopath_prefix)/$(m3_package)
mockgen_package      := github.com/golang/mock/mockgen
tools_bin_path       := $(abspath ./_tools/bin)
combined_bin_paths   := $(tools_bin_path):$(gopath_bin_path)
retool_src_prefix    := $(m3_package_path)/_tools/src
retool_package       := github.com/twitchtv/retool
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
genny_target         ?= genny-all

BUILD                     := $(abspath ./bin)
VENDOR                    := $(m3_package_path)/$(vendor_prefix)
GO_BUILD_LDFLAGS_CMD      := $(abspath ./scripts/go-build-ldflags.sh)
GO_BUILD_LDFLAGS          := $(shell $(GO_BUILD_LDFLAGS_CMD) LDFLAG)
GO_BUILD_COMMON_ENV       := CGO_ENABLED=0
LINUX_AMD64_ENV           := GOOS=linux GOARCH=amd64 $(GO_BUILD_COMMON_ENV)
# GO_RELEASER_DOCKER_IMAGE is latest goreleaser for go 1.16
GO_RELEASER_DOCKER_IMAGE  := goreleaser/goreleaser:v0.173.2
GO_RELEASER_RELEASE_ARGS  ?= --rm-dist
GO_RELEASER_WORKING_DIR   := /go/src/github.com/m3db/m3
GOLANGCI_LINT_VERSION     := v1.37.0

# Retool will look for tools.json in the nearest parent git directory if not
# explicitly told the current dir. Allow setting the base dir so that tools can
# be built inside of other external repos.
ifdef RETOOL_BASE_DIR
	retool_base_args := -base-dir $(RETOOL_BASE_DIR)
endif

export NPROC := 2 # Maximum package concurrency for unit tests.

SERVICES :=     \
	m3dbnode      \
	m3coordinator \
	m3aggregator  \
	m3query       \
	m3collector   \
	m3em_agent    \
	m3comparator  \
	r2ctl         \

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
	m3ninx      \
	aggregator  \
	ctl         \

TOOLS :=               \
	read_ids             \
	read_index_ids       \
	read_data_files      \
	read_index_files     \
	read_index_segments  \
	read_commitlog       \
	split_shards         \
	query_index_segments \
	clone_fileset        \
	dtest                \
	verify_data_files    \
	verify_index_files   \
	carbon_load          \
	m3ctl                \

GOINSTALL_BUILD_TOOLS := \
	github.com/fossas/fossa-cli/cmd/fossa@latest                                 \
	github.com/golang/mock/mockgen@latest                                        \
	github.com/google/go-jsonnet/cmd/jsonnet@latest                              \
	github.com/m3db/build-tools/utilities/genclean@latest                        \
	github.com/m3db/tools/update-license@latest                                  \
	github.com/mauricelam/genny@latest                                           \
	github.com/mjibson/esc@latest                                                \
	github.com/pointlander/peg@latest                                            \
	github.com/rakyll/statik@latest                                              \
	github.com/instrumenta/kubeval@latest                                        \
	github.com/wjdp/htmltest@latest                                              \
	github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) \

.PHONY: setup
setup:
	mkdir -p $(BUILD)

.PHONY: install-vendor-m3
install-vendor-m3:
	[ -d $(VENDOR) ] || GOSUMDB=off go mod vendor

.PHONY: docker-dev-prep
docker-dev-prep:
	mkdir -p ./bin/config

	# Hacky way to find all configs and put into ./bin/config/
	find ./src | fgrep config | fgrep ".yml" | xargs -I{} cp {} ./bin/config/
	find ./src | fgrep config | fgrep ".yaml" | xargs -I{} cp {} ./bin/config/

define SERVICE_RULES

.PHONY: $(SERVICE)
$(SERVICE): setup
ifeq ($(SERVICE),m3ctl)
	@echo "Building $(SERVICE) dependencies"
	make build-ui-ctl-statik-gen
endif
	@echo Building $(SERVICE)
	[ -d $(VENDOR) ] || make install-vendor-m3
	$(GO_BUILD_COMMON_ENV) go build -ldflags '$(GO_BUILD_LDFLAGS)' -o $(BUILD)/$(SERVICE) ./src/cmd/services/$(SERVICE)/main/.

.PHONY: $(SERVICE)-linux-amd64
$(SERVICE)-linux-amd64:
	$(LINUX_AMD64_ENV) make $(SERVICE)

.PHONY: $(SERVICE)-docker-dev
$(SERVICE)-docker-dev: clean-build $(SERVICE)-linux-amd64
	make docker-dev-prep

	# Build development docker image
	docker build -t $(SERVICE):dev -t quay.io/m3dbtest/$(SERVICE):dev-$(USER) -f ./docker/$(SERVICE)/development.Dockerfile ./bin

.PHONY: $(SERVICE)-docker-dev-push
$(SERVICE)-docker-dev-push: $(SERVICE)-docker-dev
	docker push quay.io/m3dbtest/$(SERVICE):dev-$(USER)
	@echo "Pushed quay.io/m3dbtest/$(SERVICE):dev-$(USER)"

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
all: test-ci-unit test-ci-integration services tools
	@echo Made all successfully

.SILENT: install-tools
.PHONY: install-tools
install-tools:
	echo "Installing build tools"
	for tool in $(GOINSTALL_BUILD_TOOLS); do \
		GOBIN=$(tools_bin_path) go install $$tool;  \
		done

.PHONY: check-for-goreleaser-github-token
check-for-goreleaser-github-token:
  ifndef GITHUB_TOKEN
		echo "Usage: make GITHUB_TOKEN=\"<TOKEN>\" release"
		exit 1
  endif

.PHONY: release
release: check-for-goreleaser-github-token
	@echo Releasing new version
	$(GO_BUILD_LDFLAGS_CMD) ECHO > $(BUILD)/release-vars.env
	docker run -e "GITHUB_TOKEN=$(GITHUB_TOKEN)" --env-file $(BUILD)/release-vars.env -v $(PWD):$(GO_RELEASER_WORKING_DIR) -w $(GO_RELEASER_WORKING_DIR) $(GO_RELEASER_DOCKER_IMAGE) release $(GO_RELEASER_RELEASE_ARGS)

.PHONY: release-snapshot
release-snapshot: check-for-goreleaser-github-token
	@echo Creating snapshot release
	make release GO_RELEASER_RELEASE_ARGS="--snapshot --rm-dist"

# NB(schallert): if updating this target, be sure to update the commands used in
# the .buildkite/docs_push.sh. We can't share the make targets because our
# Makefile assumes its running under bash and the container is alpine (ash
# shell).

.PHONY: docs-build
docs-build:
	@HUGO_DOCKER=true ./scripts/site-build.sh

.PHONY: docs-test
docs-test: setup install-tools docs-build
	cp site/.htmltest.yml $(BUILD)/.htmltest.yml
ifneq ($(DOCSTEST_AUTH_USER),)
ifneq ($(DOCSTEST_AUTH_TOKEN),)
	@echo 'HTTPHeaders: {"Authorization":"Basic $(shell echo -n "$$DOCSTEST_AUTH_USER:$$DOCSTEST_AUTH_TOKEN" | base64 | xargs echo -n)"}' >> $(BUILD)/.htmltest.yml
endif
endif
	$(tools_bin_path)/htmltest -c $(BUILD)/.htmltest.yml

.PHONY: docker-integration-test
docker-integration-test:
	@echo "--- Running Docker integration test"
	./scripts/docker-integration-tests/run.sh

.PHONY: docker-compatibility-test
docker-compatibility-test:
	@echo "--- Running Prometheus compatibility test"
	./scripts/comparator/run.sh

.PHONY: prom-compat
prom-compat:
	@echo "--- Running local Prometheus compatibility test"
	CI="false" make docker-compatibility-test

.PHONY: site-build
site-build:
	@echo "Building site"
	@./scripts/site-build.sh

# Generate configs in config/
.PHONY: config-gen
config-gen: install-tools
	@echo "--- Generating configs"
	$(tools_bin_path)/jsonnet -S $(m3_package_path)/config/m3db/local-etcd/m3dbnode_cmd.jsonnet > $(m3_package_path)/config/m3db/local-etcd/generated.yaml
	$(tools_bin_path)/jsonnet -S $(m3_package_path)/config/m3db/clustered-etcd/m3dbnode_cmd.jsonnet > $(m3_package_path)/config/m3db/clustered-etcd/generated.yaml

SUBDIR_TARGETS := \
	mock-gen        \
	thrift-gen      \
	proto-gen       \
	asset-gen       \
	genny-gen       \
	license-gen     \
	all-gen         \
	all-gen

.PHONY: test-ci-unit
test-ci-unit: test-base
	$(process_coverfile) $(coverfile)

.PHONY: test-ci-big-unit
test-ci-big-unit: test-big-base
	$(process_coverfile) $(coverfile)

.PHONY: test-ci-integration
test-ci-integration:
	INTEGRATION_TIMEOUT=10m TEST_SERIES_CACHE_POLICY=$(cache_policy) make test-base-ci-integration
	$(process_coverfile) $(coverfile)

define SUBDIR_RULES

.PHONY: mock-gen-$(SUBDIR)
mock-gen-$(SUBDIR): install-tools
	@echo "--- Generating mocks $(SUBDIR)"
	@[ ! -d src/$(SUBDIR)/$(mocks_rules_dir) ] || \
		PATH=$(combined_bin_paths):$(PATH) PACKAGE=$(m3_package) $(auto_gen) src/$(SUBDIR)/$(mocks_output_dir) src/$(SUBDIR)/$(mocks_rules_dir)

.PHONY: thrift-gen-$(SUBDIR)
thrift-gen-$(SUBDIR): install-tools
	@echo "--- Generating thrift files $(SUBDIR)"
	@[ ! -d src/$(SUBDIR)/$(thrift_rules_dir) ] || \
		PATH=$(combined_bin_paths):$(PATH) PACKAGE=$(m3_package) $(auto_gen) src/$(SUBDIR)/$(thrift_output_dir) src/$(SUBDIR)/$(thrift_rules_dir)

.PHONY: proto-gen-$(SUBDIR)
proto-gen-$(SUBDIR): install-tools
	@echo "--- Generating protobuf files $(SUBDIR)"
	@[ ! -d src/$(SUBDIR)/$(proto_rules_dir) ] || \
		PATH=$(combined_bin_paths):$(PATH) PACKAGE=$(m3_package) $(auto_gen) src/$(SUBDIR)/$(proto_output_dir) src/$(SUBDIR)/$(proto_rules_dir)

.PHONY: asset-gen-$(SUBDIR)
asset-gen-$(SUBDIR): install-tools
	@echo "--- Generating asset files $(SUBDIR)"
	@[ ! -d src/$(SUBDIR)/$(assets_rules_dir) ] || \
		PATH=$(combined_bin_paths):$(PATH) PACKAGE=$(m3_package) $(auto_gen) src/$(SUBDIR)/$(assets_output_dir) src/$(SUBDIR)/$(assets_rules_dir)

.PHONY: genny-gen-$(SUBDIR)
genny-gen-$(SUBDIR): install-tools
	@echo "--- Generating genny files $(SUBDIR)"
	@[ ! -f $(SELF_DIR)/src/$(SUBDIR)/generated-source-files.mk ] || \
		PATH=$(combined_bin_paths):$(PATH) make -f $(SELF_DIR)/src/$(SUBDIR)/generated-source-files.mk $(genny_target)
	@PATH=$(combined_bin_paths):$(PATH) bash -c "source ./scripts/auto-gen-helpers.sh && gen_cleanup_dir '*_gen.go' $(SELF_DIR)/src/$(SUBDIR)/ && gen_cleanup_dir '*_gen_test.go' $(SELF_DIR)/src/$(SUBDIR)/"

.PHONY: license-gen-$(SUBDIR)
license-gen-$(SUBDIR): install-tools
	@echo "--- Updating license in files $(SUBDIR)"
	@find $(SELF_DIR)/src/$(SUBDIR) -name '*.go' | PATH=$(combined_bin_paths):$(PATH) xargs -I{} update-license {}

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
	if [ -z "$(SKIP_CODECOV)" ]; then \
		@echo "--- uploading coverage report"; \
		$(codecov_push) -f $(coverfile) -F $(SUBDIR); \
	fi

.PHONY: test-ci-big-unit-$(SUBDIR)
test-ci-big-unit-$(SUBDIR):
	@echo "--- test-ci-big-unit $(SUBDIR)"
	SRC_ROOT=./src/$(SUBDIR) make test-big-base
	if [ -z "$(SKIP_CODECOV)" ]; then \
		@echo "--- uploading coverage report"; \
		$(codecov_push) -f $(coverfile) -F $(SUBDIR); \
	fi

.PHONY: test-ci-integration-$(SUBDIR)
test-ci-integration-$(SUBDIR):
	@echo "--- test-ci-integration $(SUBDIR)"
	SRC_ROOT=./src/$(SUBDIR) PANIC_ON_INVARIANT_VIOLATED=true INTEGRATION_TIMEOUT=10m TEST_SERIES_CACHE_POLICY=$(cache_policy) make test-base-ci-integration
	if [ -z "$(SKIP_CODECOV)" ]; then \
		@echo "--- uploading coverage report"; \
		$(codecov_push) -f $(coverfile) -F $(SUBDIR); \
	fi

.PHONY: lint-$(SUBDIR)
lint-$(SUBDIR): export GO_BUILD_TAGS = $(GO_BUILD_TAGS_LIST)
lint-$(SUBDIR): install-tools linter
	@echo "--- :golang: Running linters on $(SUBDIR)"
	./scripts/run-ci-lint.sh $(tools_bin_path)/golangci-lint ./src/$(SUBDIR)/...
	$(tools_bin_path)/linter ./src/$(SUBDIR)/...

endef

# generate targets for each SUBDIR in SUBDIRS based on the rules specified above.
$(foreach SUBDIR,$(SUBDIRS),$(eval $(SUBDIR_RULES)))

define SUBDIR_TARGET_RULE
.PHONY: $(SUBDIR_TARGET)
$(SUBDIR_TARGET): $(foreach SUBDIR,$(SUBDIRS),$(SUBDIR_TARGET)-$(SUBDIR))
endef

# generate targets across SUBDIRS for each SUBDIR_TARGET. i.e. generate rules
# which allow `make all-gen` to invoke `make all-gen-dbnode all-gen-coordinator ...`
# NB: we skip lint explicity as it runs as a separate CI step.
$(foreach SUBDIR_TARGET, $(SUBDIR_TARGETS), $(eval $(SUBDIR_TARGET_RULE)))

# Builds the single kube bundle from individual manifest files.
.PHONY: kube-gen-all
kube-gen-all: install-tools
	@echo "--- Generating kube bundle"
	@./kube/scripts/build_bundle.sh
	find kube -name '*.yaml' -print0 | PATH=$(combined_bin_paths):$(PATH) xargs -0 kubeval -v=1.12.0

.PHONY: go-mod-tidy
go-mod-tidy:
	@echo "--- :golang: tidying modules"
	go mod tidy

.PHONY: all-gen
all-gen: \
	install-tools \
	$(foreach SUBDIR_TARGET, $(filter-out lint all-gen,$(SUBDIR_TARGETS)), $(SUBDIR_TARGET)) \
	kube-gen-all \
	go-mod-tidy

.PHONY: build-ui-ctl
build-ui-ctl:
ifeq ($(shell ls ./src/ctl/ui/build 2>/dev/null),)
	# Need to use subshell output of set-node-version as cannot
	# set side-effects of nvm to later commands
	@echo "Building UI components, if npm install or build fails try: npm cache clean"
	make node-yarn-run \
		node_version="6" \
		node_cmd="cd $(m3_package_path)/src/ctl/ui && yarn install && yarn build"
	# If we've installed nvm locally, remove it due to some cleanup permissions
	# issue we run into in CI.
	rm -rf .nvm
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
	$(tools_bin_path)/statik -m -f -src ./src/ctl/ui/build -dest ./src/ctl/generated/ui -p statik

.PHONY: node-yarn-run
node-yarn-run:
	make node-run \
		node_version="$(node_version)" \
		node_cmd="(yarn --version 2>&1 >/dev/null || npm install -g yarn@^1.17.0) && $(node_cmd)"

.PHONY: node-run
node-run:
ifneq ($(shell command -v nvm 2>/dev/null),)
	@echo "Using nvm to select node version $(node_version)"
	nvm use $(node_version) && bash -c "$(node_cmd)"
else
	mkdir .nvm
	# Install nvm locally
	NVM_DIR=$(SELF_DIR)/.nvm PROFILE=/dev/null scripts/install_nvm.sh
	bash -c "source $(SELF_DIR)/.nvm/nvm.sh; nvm install 6"
	bash -c "source $(SELF_DIR)/.nvm/nvm.sh && nvm use 6 && $(node_cmd)"
endif

# Tests that all currently generated types match their contents if they were regenerated
.PHONY: test-all-gen
test-all-gen: all-gen
	@test "$(shell git --no-pager diff --exit-code --shortstat 2>/dev/null)" = "" || (git --no-pager diff --text --exit-code && echo "Check git status, there are dirty files" && exit 1)
	@test "$(shell git status --exit-code --porcelain 2>/dev/null | grep "^??")" = "" || (git status --exit-code --porcelain && echo "Check git status, there are untracked files" && exit 1)

# Runs a fossa license report
.PHONY: fossa
fossa: install-tools
	PATH=$(combined_bin_paths):$(PATH) fossa analyze --verbose --no-ansi

# Waits for the result of a fossa test and exits success if pass or fail if fails
.PHONY: fossa-test
fossa-test: fossa
	PATH=$(combined_bin_paths):$(PATH) fossa test

.PHONY: clean-build
clean-build:
	@rm -rf $(BUILD)

.PHONY: clean
clean: clean-build
	@rm -f *.html *.xml *.out *.test
	@rm -rf $(VENDOR)
	@rm -rf ./src/ctl/ui/build

.DEFAULT_GOAL := all

lint: install-tools
	@echo "--- :golang: Running linter on 'src'"
	./scripts/run-ci-lint.sh $(tools_bin_path)/golangci-lint ./src/...

export GO15VENDOREXPERIMENT=1
GO_VERSION := $(shell go version | awk '{ print $$3 }')
GO_MINOR_VERSION := $(word 2,$(subst ., ,$(GO_VERSION)))
LINTABLE_MINOR_VERSIONS := 6
FMTABLE_MINOR_VERSIONS := 7
ifneq ($(filter $(LINTABLE_MINOR_VERSIONS),$(GO_MINOR_VERSION)),)
SHOULD_LINT := true
endif
ifneq ($(filter $(FMTABLE_MINOR_VERSIONS),$(GO_MINOR_VERSION)),)
SHOULD_LINT_FMT := true
endif

PATH := $(GOPATH)/bin:$(PATH)
EXAMPLES=./examples/bench/server ./examples/bench/client ./examples/ping ./examples/thrift ./examples/hyperbahn/echo-server
ALL_PKGS := $(shell glide nv)
PROD_PKGS := . ./http ./hyperbahn ./json ./pprof ./raw ./relay ./stats ./thrift $(EXAMPLES)
TEST_ARG ?= -race -v -timeout 5m
BUILD := ./build
THRIFT_GEN_RELEASE := ./thrift-gen-release
THRIFT_GEN_RELEASE_LINUX := $(THRIFT_GEN_RELEASE)/linux-x86_64
THRIFT_GEN_RELEASE_DARWIN := $(THRIFT_GEN_RELEASE)/darwin-x86_64
SRCS := $(foreach pkg,$(PKGS),$(wildcard $(pkg)/*.go))

PLATFORM := $(shell uname -s | tr '[:upper:]' '[:lower:]')
ARCH := $(shell uname -m)
THRIFT_REL := ./scripts/travis/thrift-release/$(PLATFORM)-$(ARCH)

OLD_GOPATH := $(GOPATH)
VENDOR_PATH := $(PWD)/.tmp/vendor

export PATH := $(realpath $(THRIFT_REL)):$(PATH)
export GOPATH := $(VENDOR_PATH):$(GOPATH)

# Cross language test args
TEST_HOST=127.0.0.1
TEST_PORT=0

-include crossdock/rules.mk

all: test examples

packages_test:
	go list -json ./... | jq -r '. | select ((.TestGoFiles | length) > 0)  | .ImportPath'

setup:
	mkdir -p $(BUILD)
	mkdir -p $(BUILD)/examples
	mkdir -p $(THRIFT_GEN_RELEASE_LINUX)
	mkdir -p $(THRIFT_GEN_RELEASE_DARWIN)

get_thrift:
	scripts/travis/get-thrift.sh

# We want to remove `vendor` dir because thrift-gen tests don't work with it.
# However, glide install even with --cache-gopath option leaves GOPATH at HEAD,
# not at the desired versions from glide.lock, which are only applied to `vendor`
# dir. So we move `vendor` to a temp dir and prepend it to GOPATH.
# Note that glide itself is still executed against the original GOPATH.
install:
	GOPATH=$(OLD_GOPATH) glide --debug install --cache --cache-gopath
	rm -rf $(VENDOR_PATH)
	mkdir -p $(VENDOR_PATH)
	mv vendor $(VENDOR_PATH)/src

install_lint:
ifdef SHOULD_LINT
	@echo "Installing golint, since we expect to lint on" $(GO_VERSION)
	GOPATH=$(OLD_GOPATH) go get -u -f github.com/golang/lint/golint
else
	@echo "Not installing golint, since we don't lint on" $(GO_VERSION)
endif

install_glide:
	GOPATH=$(OLD_GOPATH) go get -u github.com/Masterminds/glide

install_ci: install_glide install_lint get_thrift install
	GOPATH=$(OLD_GOPATH) go get -u github.com/mattn/goveralls
ifdef CROSSDOCK
	$(MAKE) install_docker_ci
endif

install_test:
	go test -i $(TEST_ARG) $(ALL_PKGS)

help:
	@egrep "^# target:" [Mm]akefile | sort -

clean:
	echo Cleaning build artifacts...
	go clean
	rm -rf $(BUILD) $(THRIFT_GEN_RELEASE)
	echo

fmt format:
	echo Formatting Packages...
	go fmt $(ALL_PKGS)
	echo

test_ci:
ifdef CROSSDOCK
	$(MAKE) crossdock_ci
else
	$(MAKE) test
endif

test: clean setup install_test check_no_test_deps
	@echo Testing packages:
	go test -parallel=4 $(TEST_ARG) $(ALL_PKGS)
	@echo Running frame pool tests
	go test -run TestFramesReleased -stressTest $(TEST_ARG)

check_no_test_deps:
	! go list -json $(PROD_PKGS) | jq -r .Deps[] | grep -e test -e mock

benchmark: clean setup
	echo Running benchmarks:
	go test $(ALL_PKGS) -bench=. -cpu=1 -benchmem -run NONE

cover_profile: clean setup
	@echo Testing packages:
	mkdir -p $(BUILD)
	go test ./ $(TEST_ARG) -coverprofile=$(BUILD)/coverage.out

cover: cover_profile
	go tool cover -html=$(BUILD)/coverage.out

cover_ci:
ifdef CROSSDOCK
	@echo Skipping coverage
else
	$(MAKE) cover_profile
	goveralls -coverprofile=$(BUILD)/coverage.out -service=travis-ci || echo -e "\x1b[31mCoveralls failed\x1b[m"
endif


FILTER := grep -v -e '_string.go' -e '/gen-go/' -e '/mocks/' -e 'vendor/'
lint:
ifdef SHOULD_LINT
	@echo "Linters are enabled on" $(GO_VERSION)
	@echo "Running golint"
	-golint ./... | $(FILTER) | tee lint.log
	@echo "Running go vet"
	-go vet $(PKGS) 2>&1 | tee -a lint.log
ifdef SHOULD_LINT_FMT
	@echo "Checking gofmt"
	-gofmt -l . | $(FILTER) | tee -a lint.log
else
	@echo "Not checking gofmt on" $(GO_VERSION)
endif
	@echo "Checking for unresolved FIXMEs"
	-git grep -i -n fixme | $(FILTER) | grep -v -e Makefile | tee -a lint.log
	@[ ! -s lint.log ]
else
	@echo "Skipping linters on" $(GO_VERSION)
endif

thrift_example: thrift_gen
	go build -o $(BUILD)/examples/thrift       ./examples/thrift/main.go

test_server:
	./build/examples/test_server --host ${TEST_HOST} --port ${TEST_PORT}

examples: clean setup thrift_example
	echo Building examples...
	mkdir -p $(BUILD)/examples/ping $(BUILD)/examples/bench
	go build -o $(BUILD)/examples/ping/pong    ./examples/ping/main.go
	go build -o $(BUILD)/examples/hyperbahn/echo-server    ./examples/hyperbahn/echo-server/main.go
	go build -o $(BUILD)/examples/bench/server ./examples/bench/server
	go build -o $(BUILD)/examples/bench/client ./examples/bench/client
	go build -o $(BUILD)/examples/bench/runner ./examples/bench/runner.go
	go build -o $(BUILD)/examples/test_server ./examples/test_server

thrift_gen:
	go build -o $(BUILD)/thrift-gen ./thrift/thrift-gen
	$(BUILD)/thrift-gen --generateThrift --inputFile thrift/test.thrift --outputDir thrift/gen-go/
	$(BUILD)/thrift-gen --generateThrift --inputFile examples/keyvalue/keyvalue.thrift --outputDir examples/keyvalue/gen-go
	$(BUILD)/thrift-gen --generateThrift --inputFile examples/thrift/example.thrift --outputDir examples/thrift/gen-go
	$(BUILD)/thrift-gen --generateThrift --inputFile hyperbahn/hyperbahn.thrift --outputDir hyperbahn/gen-go
	rm -rf trace/thrift/gen-go/tcollector && $(BUILD)/thrift-gen --generateThrift --inputFile trace/tcollector.thrift --outputDir trace/thrift/gen-go/

release_thrift_gen: clean setup
	GOOS=linux GOARCH=amd64 go build -o $(THRIFT_GEN_RELEASE_LINUX)/thrift-gen ./thrift/thrift-gen
	GOOS=darwin GOARCH=amd64 go build -o $(THRIFT_GEN_RELEASE_DARWIN)/thrift-gen ./thrift/thrift-gen
	tar -czf thrift-gen-release.tar.gz $(THRIFT_GEN_RELEASE)
	mv thrift-gen-release.tar.gz $(THRIFT_GEN_RELEASE)/

.PHONY: all help clean fmt format get_thrift install install_ci install_lint install_glide release_thrift_gen packages_test check_no_test_deps test test_ci lint
.SILENT: all help clean fmt format test lint

BENCH_FLAGS ?= -cpuprofile=cpu.pprof -memprofile=mem.pprof -benchmem
PKGS        ?= $(shell $(GLIDE) novendor)
# Many Go tools take file globs or directories as arguments instead of packages.
PKG_FILES ?= *.go

# The linting tools evolve with each Go version, so run them only on the latest
# stable release.
GO_VERSION := $(shell go version | cut -d " " -f 3)
GO_MINOR_VERSION := $(word 2,$(subst ., ,$(GO_VERSION)))
LINTABLE_MINOR_VERSIONS := 7 8 9 10
ifneq ($(filter $(LINTABLE_MINOR_VERSIONS),$(GO_MINOR_VERSION)),)
SHOULD_LINT := true
endif

CWD          := $(shell pwd)
PACKAGE_ROOT := github.com/uber-go
PACKAGE      := $(PACKAGE_ROOT)/panicmon
GLIDEPATH    := $(shell which glide)
GOPATH 		   := $(shell go env GOPATH)
ifeq (, $(GLIDEPATH))
GLIDE := $(GOPATH)/bin/glide
else
GLIDE := $(GLIDEPATH)
endif

.PHONY: all
all: lint test

# Screw this, getting Jenkins working without internal test dependencies isn't
# worth it if we're moving to Github anyway.
.PHONY: jenkins
jenkins: setup-jenkins
	$(GOPATH)/bin/gocov test $(PACKAGE) | $(GOPATH)/bin/gocov-xml > coverage.xml
	touch junit.xml

.PHONY: setup-jenkins
setup-jenkins:
	rm -rf $(GOPATH)/src/$(PACKAGE)
	go get -u github.com/stretchr/testify/...
	go get github.com/axw/gocov
	go get github.com/AlekSi/gocov-xml
	# symlink `pwd` -> correct directory
	mkdir -p $(GOPATH)/src/$(PACKAGE_ROOT)
	ln -sf $(CWD) $(GOPATH)/src/$(PACKAGE)

.PHONY: dependencies
dependencies:
	rm -rf vendor/*
ifeq (, $(GLIDEPATH))
	@echo "Installing Glide and locked dependencies..."
	go get -u -f github.com/Masterminds/glide
endif
	$(GLIDE) install -v
	@echo "Installing test dependencies..."
	go get github.com/axw/gocov/gocov
	go get github.com/mattn/goveralls
ifdef SHOULD_LINT
	@echo "Installing golint..."
	go get github.com/golang/lint/golint
else
	@echo "Not installing golint, since we don't expect to lint on" $(GO_VERSION)
endif

# Disable printf-like invocation checking due to testify.assert.Error()
VET_RULES := -printf=false

.PHONY: lint
lint: clean
ifdef SHOULD_LINT
	@echo "Checking formatting..."
	@gofmt -d -s $(PKG_FILES) 2>&1 | tee lint.log
	@echo "Installing test dependencies for vet..."
	@go test -i $(PKGS)
	@echo "Checking vet..."
	@$(foreach dir,$(PKG_FILES),go tool vet $(VET_RULES) $(dir) 2>&1 | tee -a lint.log;)
	@echo "Checking lint..."
	@$(foreach dir,$(PKGS),golint $(dir) 2>&1 | tee -a lint.log;)
	@echo "Checking for unresolved FIXMEs..."
	@git grep -i fixme | grep -v -e vendor -e Makefile | tee -a lint.log
	@echo "Checking for license headers..."
	@./check_license.sh | tee -a lint.log
	@[ ! -s lint.log ]
else
	@echo "Skipping linters on" $(GO_VERSION)
endif

.PHONY: test
test:
	go test -race $(PKGS)

.PHONY: coveralls
coveralls:
	goveralls -service=travis-ci .

.PHONY: bench
BENCH ?= .
bench:
	@$(foreach pkg,$(PKGS),go test -bench=$(BENCH) -run="^$$" $(BENCH_FLAGS) $(pkg);)

.PHONY: clean
clean:
	@rm -rf lint.log

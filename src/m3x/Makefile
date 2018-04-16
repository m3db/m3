SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/.ci/common.mk

SHELL=/bin/bash -o pipefail

html_report           := coverage.html
test                  := .ci/test-cover.sh
convert-test-data     := .ci/convert-test-data.sh
coverfile             := cover.out
coverage_xml          := coverage.xml
junit_xml             := junit.xml
test_log              := test.log
lint_check            := .ci/lint.sh
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
gorename_package      := golang.org/x/tools/cmd/gorename
codegen_package       := github.com/m3db/m3x/codegen
ifneq ($(rename_type_prefix),)
codegen_rename_pkg    := $(codegen_package)/$(rename_type_prefix)
codegen_rename_dir    := $(gopath_prefix)/$(codegen_rename_pkg)
codegen_rename_types  := $(codegen_rename_dir)/types.go
endif

BUILD           := $(abspath ./bin)
LINUX_AMD64_ENV := GOOS=linux GOARCH=amd64 CGO_ENABLED=0

.PHONY: setup
setup:
	mkdir -p $(BUILD)

.PHONY: lint
lint:
	@which golint > /dev/null || go get -u github.com/golang/lint/golint
	$(VENDOR_ENV) $(lint_check)

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
test-ci-unit: test-internal
	@which goveralls > /dev/null || go get -u -f github.com/mattn/goveralls
	goveralls -coverprofile=$(coverfile) -service=travis-ci || echo -e "\x1b[31mCoveralls failed\x1b[m"


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

.PHONY: idhashmap-update
idhashmap-update: install-generics-bin
	@cd generics/hashmap && cat ./map.go | grep -v nolint | genny -pkg idkey gen "KeyType=ident.ID ValueType=Value" > ./idkey/map_gen.go

.PHONY: byteshashmap-update
byteshashmap-update: install-generics-bin
	@cd generics/hashmap && cat ./map.go | grep -v nolint | genny -pkg byteskey gen "KeyType=[]byte ValueType=Value" > ./byteskey/map_gen.go

.PHONY: hashmap-gen
hashmap-gen: install-generics-bin
	@cd generics/hashmap && cat ./map.go | grep -v nolint | genny -pkg $(pkg) gen "KeyType=$(key_type) ValueType=$(value_type)" > "$(out_dir:\=)/map_gen.go"
ifneq ($(rename_type_prefix),)
	make hashmap-gen-rename
endif

.PHONY: idhashmap-gen
idhashmap-gen: install-generics-bin
	@cd generics/hashmap/idkey && cat ./map_gen.go | grep -v nolint | genny -pkg $(pkg) gen "Value=$(value_type)" > "$(out_dir:\=)/map_gen.go"
	@cd generics/hashmap/idkey && cat ./new_map.go | grep -v nolint | genny -pkg $(pkg) gen "Value=$(value_type)" > "$(out_dir:\=)/new_map_gen.go"
ifneq ($(rename_type_prefix),)
	make hashmap-gen-rename
endif

.PHONY: byteshashmap-gen
byteshashmap-gen: install-generics-bin
	@cd generics/hashmap/byteskey && cat ./map_gen.go | grep -v nolint | genny -pkg $(pkg) gen "Value=$(value_type)" > "$(out_dir:\=)/map_gen.go"
	@cd generics/hashmap/byteskey && cat ./new_map.go | grep -v nolint | genny -pkg $(pkg) gen "Value=$(value_type)" > "$(out_dir:\=)/new_map_gen.go"
ifneq ($(rename_type_prefix),)
	make hashmap-gen-rename
endif

.PHONY: install-gorename-bin
install-gorename-bin:
	# Get the gorename tool for the currently installed go version (cannot pin)
	which gorename >/dev/null || (     \
		go get -u $(gorename_package) && \
		go install $(gorename_package)   \
	)

.PHONY: hashmap-gen-rename
hashmap-gen-rename: install-gorename-bin
	# Use a staging package and gorename there to avoid build errors
	# and make renaming fast, use a subdirectory of codegen to allow for multiple
	# concurrent map generations at one time since gorename is quite slow
	test -d $(codegen_rename_dir) && rm -rf $(codegen_rename_dir) || echo "No need to delete codegen"
	mkdir -p $(codegen_rename_dir)
	mv "$(out_dir:\=)/map_gen.go" "$(codegen_rename_dir)/map_gen.go"
	test -f "$(out_dir:\=)/new_map_gen.go" && mv "$(out_dir:\=)/new_map_gen.go" "$(codegen_rename_dir)/new_map_gen.go" || echo "" > /dev/null
	echo "package $(pkg)" > $(codegen_rename_types)
	echo "" >> $(codegen_rename_types)
	echo "type $(value_type) interface{}" >> $(codegen_rename_types)
ifneq ($(key_type),)
	echo "type $(key_type) interface{}" >> $(codegen_rename_types)
endif
	# Rename types in the staging package
	gorename -from "\"$(codegen_rename_pkg)\".Map" -to $(rename_type_prefix)Map
	gorename -from "\"$(codegen_rename_pkg)\".MapHash" -to $(rename_type_prefix)MapHash
	gorename -from "\"$(codegen_rename_pkg)\".HashFn" -to $(rename_type_prefix)MapHashFn
	gorename -from "\"$(codegen_rename_pkg)\".EqualsFn" -to $(rename_type_prefix)MapEqualsFn
	gorename -from "\"$(codegen_rename_pkg)\".CopyFn" -to $(rename_type_prefix)MapCopyFn
	gorename -from "\"$(codegen_rename_pkg)\".FinalizeFn" -to $(rename_type_prefix)MapFinalizeFn
	gorename -from "\"$(codegen_rename_pkg)\".MapEntry" -to $(rename_type_prefix)MapEntry
	gorename -from "\"$(codegen_rename_pkg)\".SetUnsafeOptions" -to $(rename_type_prefix)MapSetUnsafeOptions
	# Add underscore to the following internal types to avoid export regardless of if new type is exported or not
	gorename -from "\"$(codegen_rename_pkg)\".mapAlloc" -to _$(rename_type_prefix)MapAlloc
	gorename -from "\"$(codegen_rename_pkg)\".mapOptions" -to _$(rename_type_prefix)MapOptions
	gorename -from "\"$(codegen_rename_pkg)\".mapKey" -to _$(rename_type_prefix)MapKey
	gorename -from "\"$(codegen_rename_pkg)\".mapKeyOptions" -to _$(rename_type_prefix)MapKeyOptions
ifneq ($(rename_constructor),)
	gorename -from "\"$(codegen_rename_pkg)\".NewMap" -to $(rename_constructor)
endif
ifneq ($(rename_constructor_options),)
	gorename -from "\"$(codegen_rename_pkg)\".MapOptions" -to $(rename_constructor_options)
endif
	# Move back from staging package
	mv "$(codegen_rename_dir)/map_gen.go" "$(out_dir:\=)/map_gen.go"
	test -f "$(codegen_rename_dir)/new_map_gen.go" && mv "$(codegen_rename_dir)/new_map_gen.go" "$(out_dir:\=)/new_map_gen.go" || echo "" > /dev/null
	# Cleanup staging
	rm -rf $(codegen_rename_dir)

.PHONY: clean
clean:
	@rm -f *.html *.xml *.out *.test

.PHONY: all
all: lint metalint test-ci-unit
	@echo Made all successfully

.DEFAULT_GOAL := all

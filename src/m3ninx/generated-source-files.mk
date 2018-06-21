SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/../../.ci/common.mk

gopath_prefix        := $(GOPATH)/src
m3ninx_package       := github.com/m3db/m3db/src/m3ninx
m3ninx_package_path  := $(gopath_prefix)/$(m3ninx_package)
m3x_package          := github.com/m3db/m3x
m3x_package_path     := $(gopath_prefix)/$(m3x_package)
m3x_package_min_ver  := 6148700dde75adcdcc27d16fb68cee2d9d9126d8

.PHONY: install-m3x-repo
install-m3x-repo: install-glide
	# Check if repository exists, if not get it
	test -d $(m3x_package_path) || go get -u $(m3x_package)
	test -d $(m3x_package_path)/vendor || (cd $(m3x_package_path) && glide install)
	test "$(shell cd $(m3x_package_path) && git diff --shortstat 2>/dev/null)" = "" || ( \
		echo "WARNING: m3x repository is dirty, generated files might not be as expected" \
	)
	# If does exist but not at min version then update it
	(cd $(m3x_package_path) && git cat-file -t $(m3x_package_min_ver) > /dev/null) || ( \
		echo "WARNING: m3x repository is below commit $(m3x_package_min_ver), generated files might not be as expected" \
	)

# Generation rule for all generated types
.PHONY: genny-all
genny-all: genny-map-all

# Map generation rule for all generated maps
.PHONY: genny-map-all
genny-map-all:                          \
	genny-map-segment-mem-postingsmap     \
	genny-map-segment-mem-fieldsmap       \
	genny-map-segment-mem-idsmap          \
	genny-map-segment-fs-postings-offset  \
	genny-map-segment-fs-fst-terms-offset

# NB: We use (genny)[1] to combat the lack of generics in Go. It allows us
# to declare templat-ized versions of code, and specialize using code
# generation.  We have a generic HashMap<K,V> implementation in `m3x`,
# with two template variables (KeyType and ValueType), along with required
# functors (HashFn, EqualsFn, ...). Below, we generate a few specialized
# versions required in m3ninx today.
#
# If we need to generate other specialized map implementations, or other
# specialized datastructures, we should add targets similar to the ones below.
#
# [1]: https://github.com/cheekybits/genny

# Map generation rule for index/segment/mem.postingsMap
.PHONY: genny-map-segment-mem-postingsmap
genny-map-segment-mem-postingsmap: install-m3x-repo
	cd $(m3x_package_path) && make hashmap-gen           \
		pkg=mem                                            \
		key_type=[]byte                                    \
		value_type=postings.MutableList                    \
		target_package=$(m3ninx_package)/index/segment/mem \
		rename_nogen_key=true                              \
		rename_nogen_value=true                            \
		rename_type_prefix=postings
	# Rename generated map file
	mv -f $(m3ninx_package_path)/index/segment/mem/map_gen.go $(m3ninx_package_path)/index/segment/mem/postings_map_gen.go

# Map generation rule for index/segment/mem.fieldsMap
.PHONY: genny-map-segment-mem-fieldsmap
genny-map-segment-mem-fieldsmap: install-m3x-repo
	cd $(m3x_package_path) && make hashmap-gen           \
		pkg=mem                                            \
		key_type=[]byte                                    \
		value_type=*concurrentPostingsMap                  \
		value_type_alias=concurrentPostingsMap             \
		target_package=$(m3ninx_package)/index/segment/mem \
	  rename_nogen_key=true                              \
		rename_type_prefix=fields
	# Rename generated map file
	mv -f $(m3ninx_package_path)/index/segment/mem/map_gen.go $(m3ninx_package_path)/index/segment/mem/fields_map_gen.go

# Map generation rule for index/segment/mem.idsMap
.PHONY: genny-map-segment-mem-idsmap
genny-map-segment-mem-idsmap: install-m3x-repo
	cd $(m3x_package_path) && make hashmap-gen           \
		pkg=mem                                            \
		key_type=[]byte                                    \
		value_type=struct{}                                \
		target_package=$(m3ninx_package)/index/segment/mem \
	  rename_nogen_key=true                              \
	  rename_nogen_value=true                            \
		rename_type_prefix=ids
	# Rename generated map file
	mv -f $(m3ninx_package_path)/index/segment/mem/map_gen.go $(m3ninx_package_path)/index/segment/mem/ids_map_gen.go

# Map generation rule for index/segment/fs/postingsOffsetsMap
.PHONY: genny-map-segment-fs-postings-offset
genny-map-segment-fs-postings-offset: install-m3x-repo
	cd $(m3x_package_path) && make hashmap-gen           \
		pkg=fs                                             \
		key_type=doc.Field                                 \
		value_type=uint64                                  \
		target_package=$(m3ninx_package)/index/segment/fs  \
		rename_nogen_key=true                              \
		rename_nogen_value=true                            \
		rename_type_prefix=postingsOffsets
	# Rename generated map file
	mv -f $(m3ninx_package_path)/index/segment/fs/map_gen.go $(m3ninx_package_path)/index/segment/fs/postings_offsets_map_gen.go

# Map generation rule for index/segment/fs/fstTermsOffsetsMap
.PHONY: genny-map-segment-fs-fst-terms-offset
genny-map-segment-fs-fst-terms-offset: install-m3x-repo
	cd $(m3x_package_path) && make hashmap-gen           \
		pkg=fs                                             \
		key_type=[]byte                                    \
		value_type=uint64                                  \
		target_package=$(m3ninx_package)/index/segment/fs  \
		rename_nogen_key=true                              \
		rename_nogen_value=true                            \
		rename_type_prefix=fstTermsOffsets
	# Rename generated map file
	mv -f $(m3ninx_package_path)/index/segment/fs/map_gen.go $(m3ninx_package_path)/index/segment/fs/fst_terms_offsets_map_gen.go

SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/../../.ci/common.mk

gopath_prefix        := $(GOPATH)/src
m3ninx_package       := github.com/m3db/m3/src/m3ninx
m3ninx_package_path  := $(gopath_prefix)/$(m3ninx_package)
m3x_package          := github.com/m3db/m3x
m3x_package_path     := $(gopath_prefix)/$(m3x_package)
m3x_package_min_ver  := 76a586220279667a81eaaec4150de182f4d5077c

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
genny-all: genny-map-all genny-arraypool-all

# Map generation rule for all generated maps
.PHONY: genny-map-all
genny-map-all:                          \
	genny-map-segment-builder-postingsmap \
	genny-map-segment-builder-fieldsmap   \
	genny-map-segment-builder-idsmap      \
	genny-map-segment-mem-fieldsmap       \
	genny-map-segment-fst-postings-offset \
	genny-map-segment-fst-terms-offset

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

# Map generation rule for index/segment/builder.PostingsMap
.PHONY: genny-map-segment-builder-postingsmap
genny-map-segment-builder-postingsmap: install-m3x-repo
	cd $(m3x_package_path) && make byteshashmap-gen          \
		pkg=builder                                            \
		value_type=postings.MutableList                        \
		target_package=$(m3ninx_package)/index/segment/builder \
		rename_nogen_key=true                                  \
		rename_nogen_value=true                                \
		rename_type_prefix=Postings                            \
		rename_constructor=NewPostingsMap                      \
		rename_constructor_options=PostingsMapOptions
	# Rename generated map file
	mv -f $(m3ninx_package_path)/index/segment/builder/map_gen.go $(m3ninx_package_path)/index/segment/builder/postings_map_gen.go
	mv -f $(m3ninx_package_path)/index/segment/builder/new_map_gen.go $(m3ninx_package_path)/index/segment/builder/postings_map_new.go

	# Map generation rule for index/segment/builder.IDsMap
.PHONY: genny-map-segment-builder-idsmap
genny-map-segment-builder-idsmap: install-m3x-repo
	cd $(m3x_package_path) && make byteshashmap-gen          \
		pkg=builder                                            \
		value_type=struct{}                                    \
		target_package=$(m3ninx_package)/index/segment/builder \
	  rename_nogen_key=true                                  \
	  rename_nogen_value=true                                \
		rename_type_prefix=IDs                                 \
		rename_constructor=NewIDsMap                           \
		rename_constructor_options=IDsMapOptions
	# Rename generated map file
	mv -f $(m3ninx_package_path)/index/segment/builder/map_gen.go $(m3ninx_package_path)/index/segment/builder/ids_map_gen.go
	mv -f $(m3ninx_package_path)/index/segment/builder/new_map_gen.go $(m3ninx_package_path)/index/segment/builder/ids_map_new.go


# Map generation rule for index/segment/builder.fieldsMap
.PHONY: genny-map-segment-builder-fieldsmap
genny-map-segment-builder-fieldsmap: install-m3x-repo
	cd $(m3x_package_path) && make byteshashmap-gen          \
		pkg=builder                                            \
		value_type=*terms                                      \
		value_type_alias=terms                                 \
		target_package=$(m3ninx_package)/index/segment/builder \
	  rename_nogen_key=true                                  \
		rename_type_prefix=fields                              \
		rename_constructor=newFieldsMap                        \
		rename_constructor_options=fieldsMapOptions
	# Rename generated map file
	mv -f $(m3ninx_package_path)/index/segment/builder/map_gen.go $(m3ninx_package_path)/index/segment/builder/fields_map_gen.go
	mv -f $(m3ninx_package_path)/index/segment/builder/new_map_gen.go $(m3ninx_package_path)/index/segment/builder/fields_map_new.go

# Map generation rule for index/segment/mem.fieldsMap
.PHONY: genny-map-segment-mem-fieldsmap
genny-map-segment-mem-fieldsmap: install-m3x-repo
	cd $(m3x_package_path) && make byteshashmap-gen      \
		pkg=mem                                            \
		value_type=*concurrentPostingsMap                  \
		value_type_alias=concurrentPostingsMap             \
		target_package=$(m3ninx_package)/index/segment/mem \
	  rename_nogen_key=true                              \
		rename_type_prefix=fields                          \
		rename_constructor=newFieldsMap                    \
		rename_constructor_options=fieldsMapOptions
	# Rename generated map file
	mv -f $(m3ninx_package_path)/index/segment/mem/map_gen.go $(m3ninx_package_path)/index/segment/mem/fields_map_gen.go
	mv -f $(m3ninx_package_path)/index/segment/mem/new_map_gen.go $(m3ninx_package_path)/index/segment/mem/fields_map_new.go

# generation rule for all generated arraypools
.PHONY: genny-arraypool-all
genny-arraypool-all:                     \
	genny-arraypool-bytes-slice-array-pool \
	genny-arraypool-document-array-pool    \

# arraypool generation rule for ./x/bytes.SliceArrayPool
.PHONY: genny-arraypool-bytes-slice-array-pool
genny-arraypool-bytes-slice-array-pool: install-m3x-repo
	cd $(m3x_package_path) && make genny-arraypool \
	pkg=bytes                                      \
	elem_type=[]byte                               \
	target_package=$(m3ninx_package)/x/bytes       \
	out_file=slice_arraypool_gen.go                \
	rename_type_prefix=Slice                       \
	rename_type_middle=Slice                       \
	rename_constructor=NewSliceArrayPool           \

	# arraypool generation rule for ./doc.DocumentArrayPool
.PHONY: genny-arraypool-document-array-pool
genny-arraypool-document-array-pool: install-m3x-repo
	cd $(m3x_package_path) && make genny-arraypool \
	pkg=doc                                        \
	elem_type=Document                             \
	target_package=$(m3ninx_package)/doc           \
	out_file=doc_arraypool_gen.go                  \
	rename_type_prefix=Document                    \
	rename_type_middle=Document                    \
	rename_constructor=NewDocumentArrayPool        \
	rename_gen_types=true                          \



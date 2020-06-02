gopath_prefix            := $(GOPATH)/src
m3x_package              := github.com/m3db/m3/src/x
m3x_package_path         := $(gopath_prefix)/$(m3x_package)
temp_suffix              := _temp
gorename_package         := github.com/prateek/gorename
gorename_package_version := 52c7307cddd221bb98f0a3215216789f3c821b10

# Tests that all currently generated types match their contents if they were regenerated
.PHONY: test-genny-all
test-genny-all: genny-all
	@test "$(shell git --no-pager diff --shortstat 2>/dev/null)" = "" || (git --no-pager diff --no-color && echo "Check git status, there are dirty files" && exit 1)
	@test "$(shell git --no-pager status --porcelain 2>/dev/null | grep "^??")" = "" || (git status --porcelain && echo "Check git status, there are untracked files" && exit 1)

.PHONY: genny-all
genny-all: genny-map-all genny-list-all genny-arraypool-all

.PHONY: genny-map-all
genny-map-all: idhashmap-update byteshashmap-update

.PHONY: idhashmap-update
idhashmap-update:
	cd $(m3x_package_path)/generics/hashmap && cat ./map.go | grep -v nolint | genny -pkg idkey -ast gen "KeyType=ident.ID ValueType=MapValue" > ./idkey/map_gen.go

.PHONY: byteshashmap-update
byteshashmap-update:
	cd $(m3x_package_path)/generics/hashmap && cat ./map.go | grep -v nolint | genny -pkg byteskey -ast gen "KeyType=[]byte ValueType=MapValue" > ./byteskey/map_gen.go

# NB(prateek): `target_package` should not have a trailing slash
# Generic targets meant to be re-used by other users
.PHONY: hashmap-gen
hashmap-gen:
	$(eval out_dir=$(gopath_prefix)/$(target_package))
	cd $(m3x_package_path)/generics/hashmap && cat ./map.go | grep -v nolint | genny -pkg $(pkg) -ast gen "KeyType=$(key_type) ValueType=$(value_type)" > "$(out_dir)/map_gen.go"
ifneq ($(rename_type_prefix),)
	make hashmap-gen-rename
endif

.PHONY: idhashmap-gen
idhashmap-gen:
	$(eval out_dir=$(gopath_prefix)/$(target_package))
	cd $(m3x_package_path)/generics/hashmap/idkey && cat ./map_gen.go | grep -v nolint | genny -pkg $(pkg) -ast gen "MapValue=$(value_type)" > "$(out_dir:\=)/map_gen.go"
	cd $(m3x_package_path)/generics/hashmap/idkey && cat ./new_map.go | grep -v nolint | genny -pkg $(pkg) -ast gen "MapValue=$(value_type)" > "$(out_dir:\=)/new_map_gen.go"
ifneq ($(rename_type_prefix),)
	make hashmap-gen-rename rename_nogen_key="true"
endif

.PHONY: byteshashmap-gen
byteshashmap-gen:
	$(eval out_dir=$(gopath_prefix)/$(target_package))
	cd $(m3x_package_path)/generics/hashmap/byteskey && cat ./map_gen.go | grep -v nolint | genny -pkg $(pkg) -ast gen "MapValue=$(value_type)" > "$(out_dir:\=)/map_gen.go"
	cd $(m3x_package_path)/generics/hashmap/byteskey && cat ./new_map.go | grep -v nolint | genny -pkg $(pkg) -ast gen "MapValue=$(value_type)" > "$(out_dir:\=)/new_map_gen.go"
ifneq ($(rename_type_prefix),)
	make hashmap-gen-rename rename_nogen_key="true"
endif

.PHONY: hashmap-gen-rename-helper
hashmap-gen-rename-helper:
	gorename -from '"$(target_package)$(temp_suffix)".Map' -to $(rename_type_prefix)Map
	gorename -from '"$(target_package)$(temp_suffix)".MapHash' -to $(rename_type_prefix)MapHash
	gorename -from '"$(target_package)$(temp_suffix)".HashFn' -to $(rename_type_prefix)MapHashFn
	gorename -from '"$(target_package)$(temp_suffix)".EqualsFn' -to $(rename_type_prefix)MapEqualsFn
	gorename -from '"$(target_package)$(temp_suffix)".CopyFn' -to $(rename_type_prefix)MapCopyFn
	gorename -from '"$(target_package)$(temp_suffix)".FinalizeFn' -to $(rename_type_prefix)MapFinalizeFn
	gorename -from '"$(target_package)$(temp_suffix)".MapEntry' -to $(rename_type_prefix)MapEntry
	gorename -from '"$(target_package)$(temp_suffix)".SetUnsafeOptions' -to $(rename_type_prefix)MapSetUnsafeOptions
	gorename -from '"$(target_package)$(temp_suffix)".mapAlloc' -to _$(rename_type_prefix)MapAlloc
	gorename -from '"$(target_package)$(temp_suffix)".mapOptions' -to _$(rename_type_prefix)MapOptions
	gorename -from '"$(target_package)$(temp_suffix)".mapKey' -to _$(rename_type_prefix)MapKey
	gorename -from '"$(target_package)$(temp_suffix)".mapKeyOptions' -to _$(rename_type_prefix)MapKeyOptions
	[ "$(rename_constructor)" = "" ] || \
	gorename -from '"$(target_package)$(temp_suffix)".NewMap' -to '$(rename_constructor)'
	[ "$(rename_constructor_options)" = "" ] || \
	gorename -from '"$(target_package)$(temp_suffix)".MapOptions' -to '$(rename_constructor_options)'

key_type_alias   ?= $(key_type)
value_type_alias ?= $(value_type)
.PHONY: hashmap-gen-rename
hashmap-gen-rename:
	$(eval out_dir=$(gopath_prefix)/$(target_package))
	$(eval temp_outdir=$(out_dir)$(temp_suffix))
	echo $(temp_outdir)
	@if test -d $(temp_outdir); then echo "temp directory $(temp_outdir) exists, failing" ; exit 1 ; fi
	mkdir -p $(temp_outdir)
	! test -f $(out_dir)/new_map_gen.go || mv $(out_dir)/new_map_gen.go $(temp_outdir)/new_map_gen.go
ifeq ($(rename_nogen_key),)
	# allow users to short circuit the generation of key.go if they don't need it.
	echo 'package $(pkg)' > $(temp_outdir)/key.go
	echo '' >> $(temp_outdir)/key.go
	test "$(key_type_alias)" == ""  || echo "type $(key_type_alias) interface{}" >> $(temp_outdir)/key.go
endif
ifeq ($(rename_nogen_value),)
	# Allow users to short circuit the generation of value.go if they don't need it.
	echo 'package $(pkg)' > $(temp_outdir)/value.go
	echo '' >> $(temp_outdir)/value.go
	test "$(value_type_alias)" = "struct*" || echo 'type $(value_type_alias) interface{}' >> $(temp_outdir)/value.go
endif
	mv $(out_dir)/map_gen.go $(temp_outdir)/map_gen.go
	make hashmap-gen-rename-helper
	mv $(temp_outdir)/map_gen.go $(out_dir)/map_gen.go
	! test -f $(temp_outdir)/new_map_gen.go || mv $(temp_outdir)/new_map_gen.go $(out_dir)/new_map_gen.go
	rm -f $(temp_outdir)/key.go
	rm -f $(temp_outdir)/value.go
	rmdir $(temp_outdir)

# Generation rule for all generated lists
.PHONY: genny-list-all
genny-list-all:                              \
	genny-list-context-finalizeables

# List generation rule for context/finalizeablesList
.PHONY: genny-list-context-finalizeables
genny-list-context-finalizeables:
	cd $(m3x_package_path) && make genny-pooled-elem-list-gen \
		pkg=context                                           \
		elem_type=finalizeable                                \
		value_type=finalizeable                               \
		rename_type_prefix=finalizeable                       \
		rename_type_middle=Finalizeable                       \
		rename_gen_types=true                                 \
		target_package=github.com/m3db/m3/src/x/context
	# Rename generated list file
	mv -f $(m3x_package_path)/context/list_gen.go $(m3x_package_path)/context/finalizeable_list_gen.go

.PHONY: genny-arraypool-all
genny-arraypool-all: genny-arraypool-ident-tags

# arraypool generation rule for ident/tagsArrayPool
.PHONY: genny-arraypool-ident-tags
genny-arraypool-ident-tags:
	cd $(m3x_package_path) && make genny-arraypool   \
		pkg=ident                                      \
		elem_type=Tag                                  \
		target_package=$(m3x_package)/ident            \
		out_file=tag_arraypool_gen.go                  \
		rename_type_prefix=tag                         \
		rename_type_middle=Tag                         \
		rename_constructor=newTagArrayPool             \
		rename_gen_types=true

# NB(prateek): `target_package` should not have a trailing slash
# Generic arraypool generation rule
.PHONY: genny-arraypool
genny-arraypool:
	$(eval out_dir=$(gopath_prefix)/$(target_package))
	cat $(m3x_package_path)/generics/arraypool/pool.go | grep -v nolint | genny -pkg $(pkg) -ast gen "elemType=$(elem_type)" > "$(out_dir)/$(out_file)"
ifneq ($(rename_type_prefix),)
	$(eval temp_outdir=$(out_dir)$(temp_suffix))
	@if [ -d $(temp_outdir) ] ; then echo "temp directory $(temp_outdir) exists, failing" ; exit 1 ; fi
	mkdir -p $(temp_outdir)
	mv $(out_dir)/$(out_file) $(temp_outdir)/$(out_file)
	make arraypool-gen-rename out_dir=$(out_dir)
	mv $(temp_outdir)/$(out_file) $(out_dir)/$(out_file)
	rmdir $(temp_outdir)
endif

elem_type_alias ?= $(elem_type)
.PHONY: arraypool-gen-rename
arraypool-gen-rename:
	$(eval temp_outdir=$(out_dir)$(temp_suffix))
ifneq ($(rename_gen_types),)
	# Allow users to short circuit the generation of types.go if they don't need it.
	echo 'package $(pkg)' > $(temp_outdir)/types.go
	echo '' >> $(temp_outdir)/types.go
	echo "type $(elem_type_alias) interface{}" >> $(temp_outdir)/types.go
endif
	gorename -from '"$(target_package)$(temp_suffix)".elemArrayPool' -to $(rename_type_prefix)ArrayPool
	gorename -from '"$(target_package)$(temp_suffix)".elemArr' -to $(rename_type_prefix)Arr
	gorename -from '"$(target_package)$(temp_suffix)".elemArrPool' -to $(rename_type_prefix)ArrPool
	gorename -from '"$(target_package)$(temp_suffix)".elemArrayPoolOpts' -to $(rename_type_prefix)ArrayPoolOpts
	gorename -from '"$(target_package)$(temp_suffix)".elemFinalizeFn' -to $(rename_type_prefix)FinalizeFn
	gorename -from '"$(target_package)$(temp_suffix)".newElemArrayPool' -to $(rename_constructor)
	gorename -from '"$(target_package)$(temp_suffix)".defaultElemFinalizerFn' -to default$(rename_type_middle)FinalizerFn
ifneq ($(rename_gen_types),)
	rm $(temp_outdir)/types.go
endif

# NB(prateek): `target_package` should not have a trailing slash
# generic leakcheckpool generation rule
.PHONY: genny-leakcheckpool
genny-leakcheckpool:
	$(eval out_dir=$(gopath_prefix)/$(target_package))
	cat $(m3x_package_path)/generics/leakcheckpool/pool.go | grep -v nolint | genny -pkg $(pkg) -ast gen "elemType=$(elem_type) elemTypePool=$(elem_type_pool)" > "$(out_dir)/$(out_file)"

.PHONY: genny-pooled-elem-list-gen
genny-pooled-elem-list-gen:
	$(eval out_dir=$(gopath_prefix)/$(target_package))
	cd $(m3x_package_path)/generics/list && cat ./list.go | grep -v nolint | genny -pkg $(pkg) -ast gen "ValueType=$(value_type)" > "$(out_dir)/list_gen.go"
ifneq ($(rename_type_prefix),)
	$(eval temp_outdir=$(out_dir)$(temp_suffix))
	@if [ -d $(temp_outdir) ] ; then echo "temp directory $(temp_outdir) exists, failing" ; exit 1 ; fi
	mkdir -p $(temp_outdir)
	mv $(out_dir)/list_gen.go $(temp_outdir)/list_gen.go
	make list-gen-rename out_dir=$(out_dir)
	mv $(temp_outdir)/list_gen.go $(out_dir)/list_gen.go
	rmdir $(temp_outdir)
endif

elem_type_alias ?= $(elem_type)
.PHONY: list-gen-rename
list-gen-rename:
	$(eval temp_outdir=$(out_dir)$(temp_suffix))
ifneq ($(rename_gen_types),)
	# Allow users to short circuit the generation of types.go if they don't need it.
	echo 'package $(pkg)' > $(temp_outdir)/types.go
	echo '' >> $(temp_outdir)/types.go
	echo "type $(elem_type_alias) interface{}" >> $(temp_outdir)/types.go
endif
	gorename -from '"$(target_package)$(temp_suffix)".Element' -to $(rename_type_prefix)Element
	gorename -from '"$(target_package)$(temp_suffix)".List' -to $(rename_type_prefix)List
	gorename -from '"$(target_package)$(temp_suffix)".ElementPool' -to $(rename_type_prefix)ElementPool
	gorename -from '"$(target_package)$(temp_suffix)".newElementPool' -to new$(rename_type_middle)ElementPool
	gorename -from '"$(target_package)$(temp_suffix)".newList' -to new$(rename_type_middle)List
ifneq ($(rename_gen_types),)
	rm $(temp_outdir)/types.go
endif

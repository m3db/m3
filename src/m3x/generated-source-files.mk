temp_suffix              := _temp
gorename_package         := github.com/prateek/gorename
gorename_package_version := 52c7307cddd221bb98f0a3215216789f3c821b10

# Tests that all currently generated types match their contents if they were regenerated
.PHONY: test-genny-all
test-genny-all: genny-all
	@test "$(shell git diff --shortstat 2>/dev/null)" = "" || (git diff --no-color && echo "Check git status, there are dirty files" && exit 1)
	@test "$(shell git status --porcelain 2>/dev/null | grep "^??")" = "" || (git status --porcelain && echo "Check git status, there are untracked files" && exit 1)

.PHONY: genny-all
genny-all: genny-map-all genny-arraypool-all

.PHONY: genny-map-all
genny-map-all: idhashmap-update byteshashmap-update

.PHONY: idhashmap-update
idhashmap-update: install-generics-bin
	cd generics/hashmap && cat ./map.go | grep -v nolint | genny -pkg idkey gen "KeyType=ident.ID ValueType=MapValue" > ./idkey/map_gen.go

.PHONY: byteshashmap-update
byteshashmap-update: install-generics-bin
	cd generics/hashmap && cat ./map.go | grep -v nolint | genny -pkg byteskey gen "KeyType=[]byte ValueType=MapValue" > ./byteskey/map_gen.go

# NB(prateek): `target_package` should not have a trailing slash
# generic targets meant to be re-used by other users
.PHONY: hashmap-gen
hashmap-gen: install-generics-bin
	$(eval out_dir=$(gopath_prefix)/$(target_package))
	cd generics/hashmap && cat ./map.go | grep -v nolint | genny -pkg $(pkg) gen "KeyType=$(key_type) ValueType=$(value_type)" > "$(out_dir)/map_gen.go"
ifneq ($(rename_type_prefix),)
	make hashmap-gen-rename
endif

.PHONY: idhashmap-gen
idhashmap-gen: install-generics-bin
	$(eval out_dir=$(gopath_prefix)/$(target_package))
	cd generics/hashmap/idkey && cat ./map_gen.go | grep -v nolint | genny -pkg $(pkg) gen "MapValue=$(value_type)" > "$(out_dir:\=)/map_gen.go"
	cd generics/hashmap/idkey && cat ./new_map.go | grep -v nolint | genny -pkg $(pkg) gen "MapValue=$(value_type)" > "$(out_dir:\=)/new_map_gen.go"
ifneq ($(rename_type_prefix),)
	make hashmap-gen-rename
endif

.PHONY: byteshashmap-gen
byteshashmap-gen: install-generics-bin
	$(eval out_dir=$(gopath_prefix)/$(target_package))
	cd generics/hashmap/byteskey && cat ./map_gen.go | grep -v nolint | genny -pkg $(pkg) gen "MapValue=$(value_type)" > "$(out_dir:\=)/map_gen.go"
	cd generics/hashmap/byteskey && cat ./new_map.go | grep -v nolint | genny -pkg $(pkg) gen "MapValue=$(value_type)" > "$(out_dir:\=)/new_map_gen.go"
ifneq ($(rename_type_prefix),)
	make hashmap-gen-rename
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

.PHONY: hashmap-gen-rename
hashmap-gen-rename: install-gorename
	$(eval out_dir=$(gopath_prefix)/$(target_package))
	$(eval temp_outdir=$(out_dir)$(temp_suffix))
	echo $(temp_outdir)
	@if [ -d $(temp_outdir) ] ; then echo "temp directory $(temp_outdir) exists, failing" ; exit 1 ; fi
	mkdir -p $(temp_outdir)
	[ ! -f $(out_dir)/new_map_gen.go ] || mv $(out_dir)/new_map_gen.go $(temp_outdir)/new_map_gen.go
	echo 'package $(pkg)' > $(temp_outdir)/types.go
	echo '' >> $(temp_outdir)/types.go
	echo 'type $(value_type) interface{}' >> $(temp_outdir)/types.go
	[ "$(key_type)" == "" ] || echo "type $(key_type) interface{}" >> $(temp_outdir)/types.go
	mv $(out_dir)/map_gen.go $(temp_outdir)/map_gen.go
	make hashmap-gen-rename-helper
	mv $(temp_outdir)/map_gen.go $(out_dir)/map_gen.go
	[ ! -f $(temp_outdir)/new_map_gen.go ] || mv $(temp_outdir)/new_map_gen.go $(out_dir)/new_map_gen.go
	rm -f $(temp_outdir)/types.go
	rmdir $(temp_outdir)

.PHONY: genny-arraypool-all
genny-arraypool-all: genny-arraypool-context-finalizers

# arraypool generation rule for context/finalizersPool
.PHONY: genny-arraypool-context-finalizers
genny-arraypool-context-finalizers: install-generics-bin
	make genny-arraypool                        \
		pkg=context                               \
		elem_type=resource.Finalizer              \
		target_package=$(m3x_package)/context     \
		out_file=finalizers_arraypool_gen.go      \
		rename_type_middle=Finalizers             \
		rename_constructor=newFinalizersArrayPool \
		rename_type_prefix=finalizers

# NB(prateek): `target_package` should not have a trailing slash
# generic arraypool generation rule
.PHONY: genny-arraypool
genny-arraypool: install-generics-bin
	$(eval out_dir=$(gopath_prefix)/$(target_package))
	cat ./generics/arraypool/pool.go | grep -v nolint | genny -pkg $(pkg) -ast gen "elemType=$(elem_type)" > "$(out_dir)/$(out_file)"
ifneq ($(rename_type_prefix),)
	$(eval temp_outdir=$(out_dir)$(temp_suffix))
	@if [ -d $(temp_outdir) ] ; then echo "temp directory $(temp_outdir) exists, failing" ; exit 1 ; fi
	mkdir -p $(temp_outdir)
	mv $(out_dir)/$(out_file) $(temp_outdir)/$(out_file)
	make arraypool-gen-rename
	mv $(temp_outdir)/$(out_file) $(out_dir)/$(out_file)
	rmdir $(temp_outdir)
endif

.PHONY: arraypool-gen-rename
arraypool-gen-rename: install-gorename
	gorename -from '"$(target_package)$(temp_suffix)".elemArrayPool' -to $(rename_type_prefix)ArrayPool
	gorename -from '"$(target_package)$(temp_suffix)".elemArr' -to $(rename_type_prefix)Arr
	gorename -from '"$(target_package)$(temp_suffix)".elemArrPool' -to $(rename_type_prefix)ArrPool
	gorename -from '"$(target_package)$(temp_suffix)".elemArrayPoolOpts' -to $(rename_type_prefix)ArrayPoolOpts
	gorename -from '"$(target_package)$(temp_suffix)".elemFinalizeFn' -to $(rename_type_prefix)FinalizeFn
	gorename -from '"$(target_package)$(temp_suffix)".newElemArrayPool' -to $(rename_constructor)
	gorename -from '"$(target_package)$(temp_suffix)".defaultElemFinalizerFn' -to default$(rename_type_middle)FinalizerFn

install-gorename:
	$(eval gorename_dir=$(gopath_prefix)/$(gorename_package))
	@([ -d $(gorename_dir) ] && which gorename >/dev/null ) || \
   (echo "Downloading specified gorename" &&      \
		 go get -d $(gorename_package) &&             \
		cd $(gopath_prefix)/$(gorename_package) &&    \
		git checkout $(gorename_package_version) &&   \
		glide install -v && go install &&             \
		echo "Successfully installed gorename") 2>/dev/null
	@which gorename > /dev/null || (echo "gorename install failed" && exit 1)

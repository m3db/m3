m3x_package          := github.com/m3db/m3x
m3x_package_path     := $(gopath_prefix)/$(m3x_package)
m3x_package_min_ver  := 6148700dde75adcdcc27d16fb68cee2d9d9126d8

.PHONY: install-m3x-repo
install-m3x-repo: install-glide install-generics-bin
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
genny-all: genny-map-all genny-arraypool-all genny-leakcheckpool-all

# Tests that all currently generated types match their contents if they were regenerated
.PHONY: test-genny-all
test-genny-all: genny-all
	@test "$(shell git diff --shortstat 2>/dev/null)" = "" || (git diff --no-color && echo "Check git status, there are dirty files" && exit 1)
	@test "$(shell git status --porcelain 2>/dev/null | grep "^??")" = "" || (git status --porcelain && echo "Check git status, there are untracked files" && exit 1)

# Map generation rule for all generated maps
.PHONY: genny-map-all
genny-map-all:                         \
	genny-map-client-received-blocks     \
	genny-map-storage-block-retriever    \
	genny-map-storage-bootstrap-result   \
	genny-map-storage                    \
	genny-map-storage-namespace-metadata \
	genny-map-storage-repair             \
	genny-map-storage-index-results

# Map generation rule for client/receivedBlocksMap
.PHONY: genny-map-client-received-blocks
genny-map-client-received-blocks: install-m3x-repo
	cd $(m3x_package_path) && make hashmap-gen  \
		pkg=client                                \
		key_type=idAndBlockStart                  \
		value_type=receivedBlocks                 \
		target_package=$(m3db_package)/client     \
		rename_type_prefix=receivedBlocks
	# Rename generated map f	ile
	mv -f $(m3db_package_path)/client/map_gen.go $(m3db_package_path)/client/received_blocks_map_gen.go

# Map generation rule for storage/block/retrieverMap
.PHONY: genny-map-storage-block-retriever
genny-map-storage-block-retriever: install-m3x-repo
	cd $(m3x_package_path) && make idhashmap-gen     \
		pkg=block                                      \
		value_type=DatabaseBlockRetriever              \
		target_package=$(m3db_package)/storage/block   \
		rename_type_prefix=retriever                   \
		rename_constructor=newRetrieverMap             \
		rename_constructor_options=retrieverMapOptions
	# Rename both generated map and constructor files
	mv -f $(m3db_package_path)/storage/block/map_gen.go $(m3db_package_path)/storage/block/retriever_map_gen.go
	mv -f $(m3db_package_path)/storage/block/new_map_gen.go $(m3db_package_path)/storage/block/retriever_new_map_gen.go

# Map generation rule for storage/bootstrap/result/Map
.PHONY: genny-map-storage-bootstrap-result
genny-map-storage-bootstrap-result: install-m3x-repo
	cd $(m3x_package_path) && make idhashmap-gen              \
		pkg=result                                              \
		value_type=DatabaseSeriesBlocks                         \
		target_package=$(m3db_package)/storage/bootstrap/result

# Map generation rule for storage package maps (to avoid double build over each other
# when generating map source files in parallel, run these sequentially)
.PHONY: genny-map-storage
genny-map-storage:
	make genny-map-storage-database-namespaces
	make genny-map-storage-shard

# Map generation rule for storage/databaseNamespacesMap
.PHONY: genny-map-storage-database-namespaces
genny-map-storage-database-namespaces: install-m3x-repo
	cd $(m3x_package_path) && make idhashmap-gen              \
		pkg=storage                                             \
		value_type=databaseNamespace                            \
		target_package=$(m3db_package)/storage                  \
		rename_type_prefix=databaseNamespaces                   \
		rename_constructor=newDatabaseNamespacesMap             \
		rename_constructor_options=databaseNamespacesMapOptions
	# Rename both generated map and constructor files
	mv -f $(m3db_package_path)/storage/map_gen.go $(m3db_package_path)/storage/namespace_map_gen.go
	mv -f $(m3db_package_path)/storage/new_map_gen.go $(m3db_package_path)/storage/namespace_new_map_gen.go

# Map generation rule for storage/shardMap
.PHONY: genny-map-storage-shard
genny-map-storage-shard: install-m3x-repo
	cd $(m3x_package_path) && make idhashmap-gen \
		pkg=storage                                \
		value_type=shardListElement                \
		target_package=$(m3db_package)/storage     \
		rename_type_prefix=shard                   \
		rename_constructor=newShardMap             \
		rename_constructor_options=shardMapOptions
	# Rename both generated map and constructor files
	mv -f $(m3db_package_path)/storage/map_gen.go $(m3db_package_path)/storage/shard_map_gen.go
	mv -f $(m3db_package_path)/storage/new_map_gen.go $(m3db_package_path)/storage/shard_new_map_gen.go

# Map generation rule for storage/namespace/metadataMap
.PHONY: genny-map-storage-namespace-metadata
genny-map-storage-namespace-metadata: install-m3x-repo
	cd $(m3x_package_path) && make idhashmap-gen       \
		pkg=namespace                                    \
		value_type=Metadata                              \
		target_package=$(m3db_package)/storage/namespace \
		rename_type_prefix=metadata                      \
		rename_constructor=newMetadataMap                \
		rename_constructor_options=metadataMapOptions
	# Rename both generated map and constructor files
	mv -f $(m3db_package_path)/storage/namespace/map_gen.go $(m3db_package_path)/storage/namespace/metadata_map_gen.go
	mv -f $(m3db_package_path)/storage/namespace/new_map_gen.go $(m3db_package_path)/storage/namespace/metadata_new_map_gen.go

# Map generation rule for storage/repair/Map
.PHONY: genny-map-storage-repair
genny-map-storage-repair: install-m3x-repo
	cd $(m3x_package_path) && make idhashmap-gen    \
		pkg=repair                                    \
		value_type=ReplicaSeriesBlocksMetadata        \
		target_package=$(m3db_package)/storage/repair

# Map generation rule for storage/index/ResultsMap
.PHONY: genny-map-storage-index-results
genny-map-storage-index-results: install-m3x-repo
	cd $(m3x_package_path) && make hashmap-gen     \
		pkg=index                                    \
		key_type=ident.ID                            \
		value_type=ident.Tags                        \
		target_package=$(m3db_package)/storage/index \
		rename_nogen_key=true                        \
		rename_nogen_value=true                      \
		rename_type_prefix=Results
	# Rename generated map file
	mv -f $(m3db_package_path)/storage/index/map_gen.go $(m3db_package_path)/storage/index/results_map_gen.go

# generation rule for all generated arraypools
.PHONY: genny-arraypool-all
genny-arraypool-all: genny-arraypool-node-segments

# arraypool generation rule for ./network/server/tchannelthrift/node/segmentsArrayPool
.PHONY: genny-arraypool-node-segments
genny-arraypool-node-segments: install-m3x-repo
	cd $(m3x_package_path) && make genny-arraypool                    \
	pkg=node                                                          \
	elem_type=*rpc.Segments                                           \
	target_package=$(m3db_package)/network/server/tchannelthrift/node \
	out_file=segments_arraypool_gen.go                                \
	rename_type_prefix=segments                                       \
	rename_type_middle=Segments                                       \
	rename_constructor=newSegmentsArrayPool

# generation rule for all generated leakcheckpools
.PHONY: genny-leakcheckpool-all
genny-leakcheckpool-all: genny-leakcheckpool-fetch-tagged-attempt \
	genny-leakcheckpool-fetch-state                                 \
	genny-leakcheckpool-fetch-tagged-op

# leakcheckpool generation rule for ./client/fetchTaggedAttemptPool
.PHONY: genny-leakcheckpool-fetch-tagged-attempt
genny-leakcheckpool-fetch-tagged-attempt: install-m3x-repo
	cd $(m3x_package_path) && make genny-leakcheckpool      \
	pkg=client                                              \
	elem_type=*fetchTaggedAttempt                           \
	elem_type_pool=fetchTaggedAttemptPool                   \
	target_package=$(m3db_package)/client                   \
	out_file=fetch_tagged_attempt_leakcheckpool_gen_test.go

# leakcheckpool generation rule for ./client/fetchStatePool
.PHONY: genny-leakcheckpool-fetch-state
genny-leakcheckpool-fetch-state: install-m3x-repo
	cd $(m3x_package_path) && make genny-leakcheckpool \
	pkg=client                                         \
	elem_type=*fetchState                              \
	elem_type_pool=fetchStatePool                      \
	target_package=$(m3db_package)/client              \
	out_file=fetch_state_leakcheckpool_gen_test.go

# leakcheckpool generation rule for ./client/fetchTaggedOp
.PHONY: genny-leakcheckpool-fetch-tagged-op
genny-leakcheckpool-fetch-tagged-op: install-m3x-repo
	cd $(m3x_package_path) && make genny-leakcheckpool  \
	pkg=client                                          \
	elem_type=*fetchTaggedOp                            \
	elem_type_pool=fetchTaggedOpPool                    \
	target_package=$(m3db_package)/client               \
	out_file=fetch_tagged_op_leakcheckpool_gen_test.go

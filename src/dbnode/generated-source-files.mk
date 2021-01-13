SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/../../.ci/common.mk

gopath_prefix        := $(GOPATH)/src
m3db_package         := github.com/m3db/m3
m3db_package_path    := $(gopath_prefix)/$(m3db_package)
m3x_package          := github.com/m3db/m3/src/x
m3x_package_path     := $(gopath_prefix)/$(m3x_package)

# Generation rule for all generated types
.PHONY: genny-all
genny-all: genny-map-all genny-arraypool-all genny-leakcheckpool-all genny-list-all

# Map generation rule for all generated maps
.PHONY: genny-map-all
genny-map-all:                                         \
	genny-map-client-received-blocks                   \
	genny-map-storage-block-retriever                  \
	genny-map-storage-bootstrap-namespaces             \
	genny-map-storage-bootstrap-namespace-results      \
	genny-map-storage-bootstrap-result                 \
	genny-map-storage                                  \
	genny-map-storage-namespace-metadata               \
	genny-map-storage-repair                           \
	genny-map-storage-index-results                    \
	genny-map-storage-index-aggregate-values           \
	genny-map-storage-index-aggregation-results        \

# Map generation rule for client/receivedBlocksMap
.PHONY: genny-map-client-received-blocks
genny-map-client-received-blocks:
	cd $(m3x_package_path) && make hashmap-gen         \
		pkg=client                                       \
		key_type=idAndBlockStart                         \
		value_type=receivedBlocks                        \
		target_package=$(m3db_package)/src/dbnode/client \
		rename_type_prefix=receivedBlocks
	# Rename generated map file
	mv -f $(m3db_package_path)/src/dbnode/client/map_gen.go $(m3db_package_path)/src/dbnode/client/received_blocks_map_gen.go

# Map generation rule for storage/block/retrieverMap
.PHONY: genny-map-storage-block-retriever
genny-map-storage-block-retriever:
	cd $(m3x_package_path) && make idhashmap-gen              \
		pkg=block                                               \
		value_type=DatabaseBlockRetriever                       \
		target_package=$(m3db_package)/src/dbnode/storage/block \
		rename_type_prefix=retriever                            \
		rename_constructor=newRetrieverMap                      \
		rename_constructor_options=retrieverMapOptions
	# Rename both generated map and constructor files
	mv -f $(m3db_package_path)/src/dbnode/storage/block/map_gen.go $(m3db_package_path)/src/dbnode/storage/block/retriever_map_gen.go
	mv -f $(m3db_package_path)/src/dbnode/storage/block/new_map_gen.go $(m3db_package_path)/src/dbnode/storage/block/retriever_new_map_gen.go

# Map generation rule for storage/bootstrap/NamespacesMap
.PHONY: genny-map-storage-bootstrap-namespaces
genny-map-storage-bootstrap-namespaces:
	cd $(m3x_package_path) && make idhashmap-gen                    \
		pkg=bootstrap                                               \
		value_type=Namespace                                        \
		target_package=$(m3db_package)/src/dbnode/storage/bootstrap \
		rename_type_prefix=Namespaces                               \
		rename_constructor=NewNamespacesMap                         \
		rename_constructor_options=NamespacesMapOptions
	# Rename both generated map and constructor files
	mv -f $(m3db_package_path)/src/dbnode/storage/bootstrap/map_gen.go $(m3db_package_path)/src/dbnode/storage/bootstrap/namespaces_map_gen.go
	mv -f $(m3db_package_path)/src/dbnode/storage/bootstrap/new_map_gen.go $(m3db_package_path)/src/dbnode/storage/bootstrap/namespaces_new_map_gen.go

# Map generation rule for storage/bootstrap/NamespaceResultsMap
.PHONY: genny-map-storage-bootstrap-namespace-results
genny-map-storage-bootstrap-namespace-results:
	cd $(m3x_package_path) && make idhashmap-gen                    \
		pkg=bootstrap                                               \
		value_type=NamespaceResult                                  \
		target_package=$(m3db_package)/src/dbnode/storage/bootstrap \
		rename_type_prefix=NamespaceResults                         \
		rename_constructor=NewNamespaceResultsMap                   \
		rename_constructor_options=NamespaceResultsMapOptions
	# Rename both generated map and constructor files
	mv -f $(m3db_package_path)/src/dbnode/storage/bootstrap/map_gen.go $(m3db_package_path)/src/dbnode/storage/bootstrap/namespace_results_map_gen.go
	mv -f $(m3db_package_path)/src/dbnode/storage/bootstrap/new_map_gen.go $(m3db_package_path)/src/dbnode/storage/bootstrap/namespace_results_new_map_gen.go

# Map generation rule for storage/bootstrap/result/Map
.PHONY: genny-map-storage-bootstrap-result
genny-map-storage-bootstrap-result:
	cd $(m3x_package_path) && make idhashmap-gen              \
		pkg=result                                              \
		value_type=DatabaseSeriesBlocks                         \
		target_package=$(m3db_package)/src/dbnode/storage/bootstrap/result

# Map generation rule for storage package maps (to avoid double build over each other
# when generating map source files in parallel, run these sequentially)
.PHONY: genny-map-storage
genny-map-storage:                      \
	genny-map-storage-shard             \
	genny-map-storage-dirty-series      \

# Map generation rule for storage/shardMap
.PHONY: genny-map-storage-shard
genny-map-storage-shard:
	cd $(m3x_package_path) && make idhashmap-gen        \
		pkg=storage                                       \
		value_type=shardListElement                       \
		target_package=$(m3db_package)/src/dbnode/storage \
		rename_type_prefix=shard                          \
		rename_constructor=newShardMap                    \
		rename_constructor_options=shardMapOptions
	# Rename both generated map and constructor files
	mv -f $(m3db_package_path)/src/dbnode/storage/map_gen.go $(m3db_package_path)/src/dbnode/storage/shard_map_gen.go
	mv -f $(m3db_package_path)/src/dbnode/storage/new_map_gen.go $(m3db_package_path)/src/dbnode/storage/shard_new_map_gen.go

# Map generation rule for storage/namespace/metadataMap
.PHONY: genny-map-storage-namespace-metadata
genny-map-storage-namespace-metadata:
	cd $(m3x_package_path) && make idhashmap-gen                  \
		pkg=namespace                                               \
		value_type=Metadata                                         \
		target_package=$(m3db_package)/src/dbnode/namespace \
		rename_type_prefix=metadata                                 \
		rename_constructor=newMetadataMap                           \
		rename_constructor_options=metadataMapOptions
	# Rename both generated map and constructor files
	mv -f $(m3db_package_path)/src/dbnode/namespace/map_gen.go $(m3db_package_path)/src/dbnode/namespace/metadata_map_gen.go
	mv -f $(m3db_package_path)/src/dbnode/namespace/new_map_gen.go $(m3db_package_path)/src/dbnode/namespace/metadata_new_map_gen.go

# Map generation rule for storage/repair/Map
.PHONY: genny-map-storage-repair
genny-map-storage-repair:
	cd $(m3x_package_path) && make idhashmap-gen    \
		pkg=repair                                    \
		value_type=ReplicaSeriesBlocksMetadata        \
		target_package=$(m3db_package)/src/dbnode/storage/repair

# Map generation rule for persist/fs
.PHONY: genny-map-persist-fs
genny-map-persist-fs:
	cd $(m3x_package_path) && make idhashmap-gen                 \
		pkg=fs                                                   \
		value_type=checked.Bytes                                 \
		target_package=$(m3db_package)/src/dbnode/persist/fs     \
		rename_constructor=newCheckedBytesByIDMap                \
		rename_constructor_options=newCheckedBytesByIDMapOptions \
		rename_type_prefix=checkedBytes                          \
		rename_nogen_value=true
	# Rename both generated map and constructor files
	mv -f $(m3db_package_path)/src/dbnode/persist/fs/map_gen.go $(m3db_package_path)/src/dbnode/persist/fs/checked_bytes_by_id_map_gen.go
	mv -f $(m3db_package_path)/src/dbnode/persist/fs/new_map_gen.go $(m3db_package_path)/src/dbnode/persist/fs/checked_bytes_by_id_new_map_gen.go

# Map generation rule for storage/index/ResultsMap
.PHONY: genny-map-storage-index-results
genny-map-storage-index-results:
	cd $(m3x_package_path) && make byteshashmap-gen             \
		pkg=index                                               \
		value_type=doc.Document                        \
		target_package=$(m3db_package)/src/dbnode/storage/index \
		rename_nogen_key=true                                   \
		rename_nogen_value=true                                 \
		rename_type_prefix=Results
	# Rename generated map file
	mv -f $(m3db_package_path)/src/dbnode/storage/index/map_gen.go $(m3db_package_path)/src/dbnode/storage/index/results_map_gen.go

# Map generation rule for storage/index/AggregateValuesMap
.PHONY: genny-map-storage-index-aggregate-values
genny-map-storage-index-aggregate-values:
	cd $(m3x_package_path) && make hashmap-gen \
		pkg=index                                \
		key_type=ident.ID                        \
		value_type=struct{}                      \
		rename_type_prefix=AggregateValues       \
		rename_nogen_key=true                    \
		rename_nogen_value=true                  \
		target_package=$(m3db_package)/src/dbnode/storage/index
	# Rename generated map file
	mv -f $(m3db_package_path)/src/dbnode/storage/index/map_gen.go $(m3db_package_path)/src/dbnode/storage/index/aggregate_values_map_gen.go

# Map generation rule for storage/index/AggregateResultsMap
.PHONY: genny-map-storage-index-aggregation-results
genny-map-storage-index-aggregation-results: genny-map-storage-index-aggregate-values
	cd $(m3x_package_path) && make idhashmap-gen  \
		pkg=index                                   \
		value_type=AggregateValues                  \
		rename_type_prefix=AggregateResults         \
		target_package=$(m3db_package)/src/dbnode/storage/index
	# Rename generated map file
	mv -f $(m3db_package_path)/src/dbnode/storage/index/map_gen.go $(m3db_package_path)/src/dbnode/storage/index/aggregate_results_map_gen.go
	# This map has a custom constructor; delete the genny generated one
	rm -f $(m3db_package_path)/src/dbnode/storage/index/new_map_gen.go

# Map generation rule for storage/DirtySeriesMap
.PHONY: genny-map-storage-dirty-series
genny-map-storage-dirty-series:
	cd $(m3x_package_path) && make hashmap-gen            \
		pkg=storage                                       \
		key_type=idAndBlockStart                          \
		value_type=*idElement                             \
		value_type_alias=idElement                        \
		target_package=$(m3db_package)/src/dbnode/storage \
		rename_type_prefix=dirtySeries
	# Rename both generated map and constructor files
	mv -f $(m3db_package_path)/src/dbnode/storage/map_gen.go $(m3db_package_path)/src/dbnode/storage/dirty_series_map_gen.go
	# This map has a custom constructor; delete the genny generated one
	rm -f $(m3db_package_path)/src/dbnode/storage/new_map_gen.go

# Generation rule for all generated arraypools
.PHONY: genny-arraypool-all
genny-arraypool-all:                      \
	genny-arraypool-node-segments           \
	genny-arraypool-aggregate-results-entry \

# arraypool generation rule for ./network/server/tchannelthrift/node/segmentsArrayPool
.PHONY: genny-arraypool-node-segments
genny-arraypool-node-segments:
	cd $(m3x_package_path) && make genny-arraypool                               \
	pkg=node                                                                     \
	elem_type=*rpc.Segments                                                      \
	target_package=$(m3db_package)/src/dbnode/network/server/tchannelthrift/node \
	out_file=segments_arraypool_gen.go                                           \
	rename_type_prefix=segments                                                  \
	rename_type_middle=Segments                                                  \
	rename_constructor=newSegmentsArrayPool

# arraypool generation rule for ./storage/index/AggregateResultsEntryArrayPool
.PHONY: genny-arraypool-aggregate-results-entry
genny-arraypool-aggregate-results-entry:
	cd $(m3x_package_path) && make genny-arraypool          \
	pkg=index                                               \
	elem_type=AggregateResultsEntry                         \
	target_package=$(m3db_package)/src/dbnode/storage/index \
	out_file=aggregate_results_entry_arraypool_gen.go       \
	rename_type_prefix=AggregateResultsEntry                \
	rename_type_middle=AggregateResultsEntry                \
	rename_constructor=NewAggregateResultsEntryArrayPool    \
	rename_gen_types=true                                   \

# generation rule for all generated leakcheckpools
.PHONY: genny-leakcheckpool-all
genny-leakcheckpool-all:                   \
	genny-leakcheckpool-fetch-tagged-attempt \
	genny-leakcheckpool-fetch-state          \
	genny-leakcheckpool-fetch-tagged-op      \
	genny-leakcheckpool-aggregate-attempt    \
	genny-leakcheckpool-aggregate-op

# leakcheckpool generation rule for ./client/fetchTaggedAttemptPool
.PHONY: genny-leakcheckpool-fetch-tagged-attempt
genny-leakcheckpool-fetch-tagged-attempt:
	cd $(m3x_package_path) && make genny-leakcheckpool      \
	pkg=client                                              \
	elem_type=*fetchTaggedAttempt                           \
	elem_type_pool=fetchTaggedAttemptPool                   \
	target_package=$(m3db_package)/src/dbnode/client        \
	out_file=fetch_tagged_attempt_leakcheckpool_gen_test.go

# leakcheckpool generation rule for ./client/fetchStatePool
.PHONY: genny-leakcheckpool-fetch-state
genny-leakcheckpool-fetch-state:
	cd $(m3x_package_path) && make genny-leakcheckpool \
	pkg=client                                         \
	elem_type=*fetchState                              \
	elem_type_pool=fetchStatePool                      \
	target_package=$(m3db_package)/src/dbnode/client   \
	out_file=fetch_state_leakcheckpool_gen_test.go

# leakcheckpool generation rule for ./client/fetchTaggedOp
.PHONY: genny-leakcheckpool-fetch-tagged-op
genny-leakcheckpool-fetch-tagged-op:
	cd $(m3x_package_path) && make genny-leakcheckpool  \
	pkg=client                                          \
	elem_type=*fetchTaggedOp                            \
	elem_type_pool=fetchTaggedOpPool                    \
	target_package=$(m3db_package)/src/dbnode/client    \
	out_file=fetch_tagged_op_leakcheckpool_gen_test.go

# leakcheckpool generation rule for ./client/aggregateOp
.PHONY: genny-leakcheckpool-aggregate-op
genny-leakcheckpool-aggregate-op:
	cd $(m3x_package_path) && make genny-leakcheckpool  \
	pkg=client                                          \
	elem_type=*aggregateOp                              \
	elem_type_pool=aggregateOpPool                      \
	target_package=$(m3db_package)/src/dbnode/client    \
	out_file=aggregate_leakcheckpool_gen_test.go

# leakcheckpool generation rule for ./client/aggregateAttemptPool
.PHONY: genny-leakcheckpool-aggregate-attempt
genny-leakcheckpool-aggregate-attempt:
	cd $(m3x_package_path) && make genny-leakcheckpool      \
	pkg=client                                              \
	elem_type=*aggregateAttempt                             \
	elem_type_pool=aggregateAttemptPool                     \
	target_package=$(m3db_package)/src/dbnode/client        \
	out_file=aggregate_attempt_leakcheckpool_gen_test.go

# Generation rule for all generated lists
.PHONY: genny-list-all
genny-list-all:                              \
	genny-list-storage-id

# List generation rule for storage/idList
.PHONY: genny-list-storage-id
genny-list-storage-id:
	cd $(m3x_package_path) && make genny-pooled-elem-list-gen \
		pkg=storage                                           \
		value_type=doc.Metadata                               \
		rename_type_prefix=id                                 \
		rename_type_middle=ID                                 \
		target_package=github.com/m3db/m3/src/dbnode/storage
	# Rename generated list file
	mv -f $(m3db_package_path)/src/dbnode/storage/list_gen.go $(m3db_package_path)/src/dbnode/storage/id_list_gen.go

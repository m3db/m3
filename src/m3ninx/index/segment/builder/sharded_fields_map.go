package builder

type shardedFieldsMap struct {
	data []*fieldsMap
}

func newShardedFieldsMap(
	numShards int,
	shardInitialCapacity int,
) *shardedFieldsMap {
	data := make([]*fieldsMap, 0, numShards)
	for i := 0; i < numShards; i++ {
		data = append(data, newFieldsMap(fieldsMapOptions{
			InitialSize: shardInitialCapacity,
		}))
	}
	return &shardedFieldsMap{
		data: data,
	}
}

func (s *shardedFieldsMap) Get(k []byte) (*terms, bool) {
	for _, fieldMap := range s.data {
		t, found := fieldMap.Get(k)
		if found {
			return t, found
		}
	}
	return nil, false
}

func (s *shardedFieldsMap) ShardedGet(
	shard int,
	k []byte,
) (*terms, bool) {
	return s.data[shard].Get(k)
}

func (s *shardedFieldsMap) ShardedSetUnsafe(
	shard int,
	k []byte,
	v *terms,
	opts fieldsMapSetUnsafeOptions,
) {
	s.data[shard].SetUnsafe(k, v, opts)
}

// ResetTerms keeps fields around but resets the terms set for each one.
func (s *shardedFieldsMap) ResetTermsSets() {
	for _, fieldMap := range s.data {
		for _, entry := range fieldMap.Iter() {
			entry.Value().reset()
		}
	}
}

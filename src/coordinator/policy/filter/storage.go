package filter

import "github.com/m3db/m3coordinator/storage"

// Querier determines whether storage can fulfil the read query
type Querier func(query *storage.ReadQuery, store storage.Storage) bool

// Appender determines whether storage can fulfil the write query
type Appender func(query *storage.WriteQuery, store storage.Storage) bool

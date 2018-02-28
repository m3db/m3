package filter

import "github.com/m3db/m3coordinator/storage"

// Storage determines whether storage can fulfil the read query
type Storage func(query storage.Query, store storage.Storage) bool

// LocalOnly filters out all remote storages
func LocalOnly(query storage.Query, store storage.Storage) bool {
	return store.Type() == storage.TypeLocalDC
}

// AllowAll does not filter any storages
func AllowAll(_ storage.Query, _ storage.Storage) bool {
	return true
}

// AllowNone filters all storages
func AllowNone(_ storage.Query, _ storage.Storage) bool {
	return false
}

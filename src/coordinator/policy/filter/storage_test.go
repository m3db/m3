package filter

import (
	"context"
	"testing"

	"github.com/m3db/m3coordinator/storage"
	"github.com/stretchr/testify/assert"
)

type mockStore struct {
	storageType storage.Type
}

func (m *mockStore) Fetch(_ context.Context, _ *storage.FetchQuery, _ *storage.FetchOptions) (*storage.FetchResult, error) {
	return nil, nil
}

func (m *mockStore) FetchTags(_ context.Context, _ *storage.FetchQuery, _ *storage.FetchOptions) (*storage.SearchResults, error) {
	return nil, nil
}

func (m *mockStore) Write(_ context.Context, _ *storage.WriteQuery) error {
	return nil
}

func (m *mockStore) Type() storage.Type {
	return m.storageType
}

var (
	local  = &mockStore{storageType: storage.TypeLocalDC}
	remote = &mockStore{storageType: storage.TypeRemoteDC}
	multi  = &mockStore{storageType: storage.TypeMultiDC}

	q = &storage.FetchQuery{}
)

func TestLocalOnly(t *testing.T) {
	assert.True(t, LocalOnly(q, local))
	assert.False(t, LocalOnly(q, remote))
	assert.False(t, LocalOnly(q, multi))
}

func TestAllowAll(t *testing.T) {
	assert.True(t, AllowAll(q, local))
	assert.True(t, AllowAll(q, remote))
	assert.True(t, AllowAll(q, multi))
}

func TestAllowNone(t *testing.T) {
	assert.False(t, AllowNone(q, local))
	assert.False(t, AllowNone(q, remote))
	assert.False(t, AllowNone(q, multi))
}

// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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

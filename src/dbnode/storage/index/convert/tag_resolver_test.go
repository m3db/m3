// Copyright (c) 2021 Uber Technologies, Inc.
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

package convert

import (
	"encoding/base64"
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/ident"

	"github.com/stretchr/testify/require"
)

//nolint:lll
const encodedTagSample = "dScMAAgAX19uYW1lX18GAGRpc2tpbwQAYXJjaAMAeDY0CgBkYXRhY2VudGVyCgB1cy13ZXN0LTJjCABob3N0bmFtZQcAaG9zdF83OAsAbWVhc3VyZW1lbnQFAHJlYWRzAgBvcwsAVWJ1bnR1MTUuMTAEAHJhY2sCADg3BgByZWdpb24JAHVzLXdlc3QtMgcAc2VydmljZQIAMTETAHNlcnZpY2VfZW52aXJvbm1lbnQKAHByb2R1Y3Rpb24PAHNlcnZpY2VfdmVyc2lvbgEAMQQAdGVhbQIAU0Y="

func TestEncodedTagsMetadataResolver(t *testing.T) {
	encodedTags, err := base64.StdEncoding.DecodeString(encodedTagSample)
	require.NoError(t, err)

	sut := NewEncodedTagsMetadataResolver(encodedTags)
	metadata, err := sut.Resolve(ident.StringID("testId"))
	require.NoError(t, err)
	require.Equal(t, "testId", string(metadata.ID))
	assertFieldValue(t, metadata, "__name__", "diskio")
}

func TestIterMetadataResolver(t *testing.T) {
	sut := NewTagsIterMetadataResolver(ident.NewTagsIterator(ident.NewTags(
		ident.StringTag("name", "foo"),
		ident.StringTag("host", "localhost"))))

	metadata, err := sut.Resolve(ident.StringID("testId"))
	require.NoError(t, err)
	require.Equal(t, "testId", string(metadata.ID))
	assertFieldValue(t, metadata, "name", "foo")
	assertFieldValue(t, metadata, "host", "localhost")
}

func TestTagsMetadataResolver(t *testing.T) {
	sut := NewTagsMetadataResolver(ident.NewTags(
		ident.StringTag("__name__", "foo"),
		ident.StringTag("name", "bar")))

	metadata, err := sut.Resolve(ident.StringID("testId"))
	require.NoError(t, err)
	require.Equal(t, "testId", string(metadata.ID))
	assertFieldValue(t, metadata, "name", "bar")
	assertFieldValue(t, metadata, "__name__", "foo")
}

func assertFieldValue(t *testing.T, metadata doc.Metadata, expectedFieldName, expectedValue string) {
	val, ok := metadata.Get([]byte(expectedFieldName))
	require.True(t, ok)
	require.Equal(t, expectedValue, string(val))
}

// Copyright (c) 2019 Uber Technologies, Inc.
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

package namespace_test

import (
	"bytes"
	"testing"

	testproto "github.com/m3db/m3/src/dbnode/generated/proto/schema_test"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getTestSchema() namespace.Schema {
	tm := &testproto.TestMessage{}
	bytes, _ := tm.Descriptor()
	ts, _ := namespace.ToSchema(bytes)
	return ts
}

func TestSchemaToFromBytes(t *testing.T) {
	inschema := getTestSchema()
	outschema, err := namespace.ToSchema(inschema.Bytes())
	assert.NoError(t, err)

	require.EqualValues(t, "TestMessage", outschema.Get().GetName())
	require.EqualValues(t, inschema.String(), outschema.String())
	require.True(t, inschema.Equal(outschema))
	require.True(t, bytes.Equal(inschema.Bytes(), outschema.Bytes()))
}

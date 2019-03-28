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
	"testing"

	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	testproto "github.com/m3db/m3/src/dbnode/generated/proto/schema_test"
	"github.com/stretchr/testify/require"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
)

func getTestSchemaOptions() []*nsproto.SchemaOption {
	tm := &testproto.TestMessage{}
	def, _ := tm.Descriptor()
	return []*nsproto.SchemaOption{{Version:1, Message:"TestMessage", Definition:def}}
}

func TestSchemaFromOption(t *testing.T) {
	testSchemaOptions := getTestSchemaOptions()
	testSchema, err := namespace.ToSchemaList(testSchemaOptions)
	require.NoError(t, err)

	require.Len(t, testSchema, 1)
	require.EqualValues(t, 1, testSchema[0].Version())
	require.EqualValues(t, "TestMessage", testSchema[0].Get().GetName())
}

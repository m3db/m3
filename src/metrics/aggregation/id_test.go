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

package aggregation

import (
	"encoding/json"
	"testing"

	"github.com/m3db/m3/src/metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3/src/x/test/testmarshal"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"

	"github.com/stretchr/testify/require"
)

var (
	testID      = ID{6}
	testIDProto = aggregationpb.AggregationID{Id: 6}
)

func TestIDToProto(t *testing.T) {
	var pb aggregationpb.AggregationID
	testID.ToProto(&pb)
	require.Equal(t, testIDProto, pb)
}

func TestIDFromProto(t *testing.T) {
	var res ID
	res.FromProto(testIDProto)
	require.Equal(t, testID, res)
}

func TestIDRoundTrip(t *testing.T) {
	var (
		pb  aggregationpb.AggregationID
		res ID
	)
	testID.ToProto(&pb)
	res.FromProto(pb)
	require.Equal(t, testID, res)
}

func TestIDMarshalJSON(t *testing.T) {
	inputs := []struct {
		id       ID
		expected string
	}{
		{id: DefaultID, expected: `null`},
		{id: testID, expected: `["Last","Min"]`},
	}
	for _, input := range inputs {
		b, err := json.Marshal(input.id)
		require.NoError(t, err)
		require.Equal(t, input.expected, string(b))
	}
}

func TestIDMarshalJSONError(t *testing.T) {
	inputs := []ID{
		ID{1234235235},
	}
	for _, input := range inputs {
		_, err := json.Marshal(input)
		require.Error(t, err)
	}
}

func TestIDMarshalRoundtrip(t *testing.T) {
	inputs := []ID{
		DefaultID,
		testID,
	}
	testmarshal.TestMarshalersRoundtrip(t, inputs, []testmarshal.Marshaler{testmarshal.YAMLMarshaler, testmarshal.JSONMarshaler})
}

func TestIDYAMLMarshaling(t *testing.T) {
	cases := []struct {
		In       ID
		Expected string
	}{{
		In: MustCompressTypes(Last, Min),
		Expected: `- Last
- Min
`,
	}}

	t.Run("marshal", func(t *testing.T) {
		for _, tc := range cases {
			testmarshal.AssertMarshals(t, testmarshal.YAMLMarshaler, tc.In, []byte(tc.Expected))
		}
	})

	t.Run("unmarshal", func(t *testing.T) {
		for _, tc := range cases {
			testmarshal.AssertUnmarshals(t, testmarshal.YAMLMarshaler, tc.In, []byte(tc.Expected))
		}
	})

	t.Run("marshal_error", func(t *testing.T) {
		_, err := yaml.Marshal(ID{40559696})
		assert.Error(t, err)
	})

	t.Run("unmarshal_error", func(t *testing.T) {
		var id ID
		err := yaml.Unmarshal([]byte(`["Foobar"]`), &id)
		assert.EqualError(t, err, "invalid aggregation type: Foobar")
	})
}

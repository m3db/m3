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

package proto

import (
	"os"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/x/xio"
)

// TestIteratorHandlesCorruptStreams ensures that the protobuf iterator never panics when reading corrupt streams.
// It determines this by generating many different random bytes streams and ensuring the protobuf iterator can
// attempt to read them without panicing.
func TestIteratorHandlesCorruptStreams(t *testing.T) {
	var (
		parameters = gopter.DefaultTestParameters()
		seed       = time.Now().UnixNano()
		props      = gopter.NewProperties(parameters)
		reporter   = gopter.NewFormatedReporter(true, 160, os.Stdout)
	)
	parameters.MinSuccessfulTests = 1000
	parameters.Rng.Seed(seed)

	props.Property("Iterator should handle corrupt streams", prop.ForAll(func(input corruptionPropTestInput) (bool, error) {
		r := xio.NewBytesReader64(input.bytes)
		iter := NewIterator(r, namespace.GetTestSchemaDescr(testVLSchema), testEncodingOptions)
		for iter.Next() {
		}
		return true, nil
	}, genIterCorruptionPropTestInput()))

	if !props.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func genIterCorruptionPropTestInput() gopter.Gen {
	return gopter.CombineGens(
		gopter.Gen(gen.SliceOfN(1024, gen.UInt8())),
	).Map(func(input []interface{}) corruptionPropTestInput {
		return corruptionPropTestInput{
			bytes: input[0].([]byte),
		}
	})
}

type corruptionPropTestInput struct {
	bytes []byte
}

// Copyright (c) 2017 Uber Technologies, Inc.
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
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/x/ident"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

const (
	testRandomSeeed        int64 = 7823434
	testMinSuccessfulTests       = 1000
)

func TestConvert(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.Rng.Seed(testRandomSeeed) // generate reproducible results
	parameters.MinSuccessfulTests = testMinSuccessfulTests
	parameters.MaxSize = 10
	parameters.MinSize = 2
	props := gopter.NewProperties(parameters)

	props.Property("Conversion rooted at metadata is bijective", prop.ForAll(
		func(nsMap namespace.Map) (bool, error) {
			reg, err := namespace.ToProto(nsMap)
			require.NoError(t, err)
			cmd, err := namespace.FromProto(*reg)
			if err != nil {
				return false, err
			}
			return nsMap.Equal(cmd), nil
		},
		genMap(),
	))
	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !props.Run(reporter) {
		t.Errorf("failed with initial seed: %d", 0) // seed)
	}
}

// map generator
func genMap() gopter.Gen {
	return gen.SliceOf(genMetadata()).Map(
		func(metadatas []namespace.Metadata) namespace.Map {
			nsMap, err := namespace.NewMap(metadatas)
			if err != nil {
				panic(err.Error())
			}
			return nsMap
		})
}

// metadata generator
func genMetadata() gopter.Gen {
	return gopter.CombineGens(
		gen.Identifier(),
		gen.SliceOfN(7, gen.Bool()),
		genRetention(),
	).Map(func(values []interface{}) namespace.Metadata {
		var (
			id        = values[0].(string)
			bools     = values[1].([]bool)
			retention = values[2].(retention.Options)
		)
		testSchemaReg, _ := namespace.LoadSchemaHistory(testSchemaOptions)
		md, err := namespace.NewMetadata(ident.StringID(id), namespace.NewOptions().
			SetBootstrapEnabled(bools[0]).
			SetCleanupEnabled(bools[1]).
			SetFlushEnabled(bools[2]).
			SetRepairEnabled(bools[3]).
			SetWritesToCommitLog(bools[4]).
			SetSnapshotEnabled(bools[5]).
			SetSchemaHistory(testSchemaReg).
			SetRetentionOptions(retention).
			SetIndexOptions(namespace.NewIndexOptions().
				SetEnabled(bools[6]).
				SetBlockSize(retention.BlockSize())))
		if err != nil {
			panic(err.Error())
		}
		return md
	})
}

func newRandomRetention(rng *rand.Rand) *generatedRetention {
	var (
		blockSizeMins    = maxInt(1, rng.Intn(60*12)) // 12 hours
		retentionMins    = maxInt(1, rng.Intn(40)) * blockSizeMins
		bufferPastMins   = maxInt(1, rng.Intn(blockSizeMins))
		bufferFutureMins = maxInt(1, rng.Intn(blockSizeMins))
	)

	return &generatedRetention{retention.NewOptions().
		SetRetentionPeriod(time.Duration(retentionMins) * time.Minute).
		SetBlockSize(time.Duration(blockSizeMins) * time.Minute).
		SetBufferPast(time.Duration(bufferPastMins) * time.Minute).
		SetBufferFuture(time.Duration(bufferFutureMins) * time.Minute)}
}

// generator for retention options
func genRetention() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		opts := newRandomRetention(genParams.Rng)
		genResult := gopter.NewGenResult(opts, gopter.NoShrinker)
		genResult.Sieve = func(v interface{}) bool {
			return v.(retention.Options).Validate() == nil
		}
		return genResult
	}
}

type generatedRetention struct {
	retention.Options
}

func (ro *generatedRetention) String() string {
	return fmt.Sprintf(
		"[ retention-period = %v, block-size = %v, buffer-past = %v, buffer-future = %v ]",
		ro.RetentionPeriod().String(),
		ro.BlockSize().String(),
		ro.BufferPast().String(),
		ro.BufferFuture().String())
}

func maxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

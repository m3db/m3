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

package cache

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"go.uber.org/atomic"
)

func TestLRUPropertyTest(t *testing.T) {
	testLRUPropFunc := func(input propTestMultiInput) (bool, error) {
		size := 1000

		cacheOpts := &LRUOptions{
			MaxEntries: size,
			Now:        time.Now,
		}

		lru := NewLRU(cacheOpts)

		// Print stats.
		for i, in := range input.inputs {
			vals := make(map[string]string)
			gets := 0
			puts := 0
			for _, op := range in.operations.operations {
				vals[op.key] = op.value
				switch op.operation {
				case propTestOperationGet:
					gets++
				case propTestOperationPut:
					puts++
				}
			}

			t.Logf("concurrency[%d] = keys=%d, gets=%d, puts=%d\n", i, len(vals), gets, puts)
		}

		// Kick off concurrent tests.
		var (
			wg       sync.WaitGroup
			found    = atomic.NewInt64(0)
			notFound = atomic.NewInt64(0)
		)
		for _, in := range input.inputs {
			in := in
			wg.Add(1)
			go func() {
				defer wg.Done()

				for _, op := range in.operations.operations {
					switch op.operation {
					case propTestOperationGet:
						_, ok := lru.TryGet(op.key)
						if !ok {
							notFound.Inc()
						} else {
							found.Inc()
						}
					case propTestOperationPut:
						lru.Put(op.key, op.value)
					default:
						panic("unknown op")
					}
				}
			}()
		}

		wg.Wait()

		t.Logf("found=%d, not_found=%d\n", found.Load(), notFound.Load())

		return true, nil
	}

	parameters := gopter.DefaultTestParameters()
	parameters.Rng.Seed(123456789)
	parameters.MinSuccessfulTests = 25
	props := gopter.NewProperties(parameters)
	props.Property("Test LRU concurrent use",
		prop.ForAll(testLRUPropFunc, genPropTestMultiInputs(propTestMultiInputsOptions{
			inputsOpts: []genPropTestInputOptions{
				{
					numKeysMin:   5000,
					numKeysMax:   7000,
					opsPerKeyMin: 8,
					opsPerKeyMax: 16,
					getPutRatio:  0.25,
				},
				{
					numKeysMin:   5000,
					numKeysMax:   7000,
					opsPerKeyMin: 3,
					opsPerKeyMax: 5,
					getPutRatio:  0.75,
				},
			},
		})))

	props.TestingRun(t)
}

type propTestInput struct {
	operations *generatedPropTestOperations
}

type propTestMultiInput struct {
	inputs []propTestInput
}

type propTestMultiInputsOptions struct {
	inputsOpts []genPropTestInputOptions
}

func genPropTestMultiInputs(opts propTestMultiInputsOptions) gopter.Gen {
	// nolint: prealloc
	var gens []gopter.Gen
	for _, subOpts := range opts.inputsOpts {
		gens = append(gens, genPropTestInput(subOpts))
	}
	return gopter.CombineGens(gens...).FlatMap(func(input interface{}) gopter.Gen {
		inputs := input.([]interface{})

		var converted []propTestInput
		for _, input := range inputs {
			converted = append(converted, input.(propTestInput))
		}

		return gopter.CombineGens(gen.Bool()).
			Map(func(input interface{}) propTestMultiInput {
				return propTestMultiInput{inputs: converted}
			})
	}, reflect.TypeOf(propTestMultiInput{}))
}

type genPropTestInputOptions struct {
	numKeysMin   int
	numKeysMax   int
	opsPerKeyMin int
	opsPerKeyMax int
	getPutRatio  float64
}

func genPropTestInput(opts genPropTestInputOptions) gopter.Gen {
	return gopter.CombineGens(
		genPropTestOperations(opts),
	).Map(func(inputs []interface{}) propTestInput {
		return propTestInput{
			operations: inputs[0].(*generatedPropTestOperations),
		}
	})
}

func genPropTestOperations(opts genPropTestInputOptions) gopter.Gen {
	return gopter.CombineGens(
		gen.IntRange(opts.numKeysMin, opts.numKeysMax),
		gen.Int64Range(math.MinInt64, math.MaxInt64),
	).FlatMap(func(input interface{}) gopter.Gen {
		inputs := input.([]interface{})

		numKeys := inputs[0].(int)

		randSeed := inputs[1].(int64)
		// nolint: gosec
		randGen := rand.New(rand.NewSource(randSeed))

		return gopter.CombineGens().Map(func(input interface{}) *generatedPropTestOperations {
			operations := make([]propTestOperation, 0, numKeys)
			for i := 0; i < numKeys; i++ {
				key := fmt.Sprintf("key-%d", i)
				value := fmt.Sprintf("value-%d", i)
				base := opts.opsPerKeyMin
				n := randGen.Intn(opts.opsPerKeyMax - base)
				for j := 0; j < base+n; j++ {
					var op propTestOperationType
					dice := randGen.Float64()
					if dice < opts.getPutRatio {
						op = propTestOperationGet
					} else {
						op = propTestOperationPut
					}

					operations = append(operations, propTestOperation{
						operation: op,
						key:       key,
						value:     value,
					})
				}
			}

			rand.Shuffle(len(operations), func(i, j int) {
				operations[i], operations[j] = operations[j], operations[i]
			})

			return &generatedPropTestOperations{
				operations: operations,
			}
		})
	}, reflect.TypeOf(&generatedPropTestOperations{}))
}

type generatedPropTestOperations struct {
	operations []propTestOperation
}

type propTestOperation struct {
	operation propTestOperationType
	key       string
	value     string
}

type propTestOperationType uint

const (
	propTestOperationGet propTestOperationType = iota
	propTestOperationPut
)

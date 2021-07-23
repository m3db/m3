// Copyright (c) 2016 Uber Technologies, Inc.
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

package checked

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	xresource "github.com/m3db/m3/src/x/resource"
)

func TestRefCountNegativeRefCount(t *testing.T) {
	elem := &RefCount{}

	var err error
	SetPanicFn(func(e error) {
		err = e
	})
	defer ResetPanicFn()

	elem.IncRef()
	assert.Equal(t, 1, elem.NumRef())
	assert.Nil(t, err)

	elem.DecRef()
	assert.Equal(t, 0, elem.NumRef())
	assert.Nil(t, err)

	elem.DecRef()
	assert.Equal(t, -1, elem.NumRef())
	assert.Error(t, err)
	assert.Equal(t, "negative ref count, ref=-1", err.Error())
}

func TestRefCountFinalizeBeforeZeroRef(t *testing.T) {
	elem := &RefCount{}

	var err error
	SetPanicFn(func(e error) {
		err = e
	})
	defer ResetPanicFn()

	elem.IncRef()
	elem.IncRef()
	assert.Nil(t, err)

	elem.Finalize()
	assert.Error(t, err)
	assert.Equal(t, "finalize before zero ref count, ref=2", err.Error())
}

func TestRefCountFinalizeCallsFinalizer(t *testing.T) {
	elem := &RefCount{}

	onFinalizeCalls := 0
	onFinalize := OnFinalize(OnFinalizeFn(func() {
		onFinalizeCalls++
	}))
	elem.SetOnFinalize(onFinalize)
	assert.Equal(t,
		reflect.ValueOf(onFinalize).Pointer(),
		reflect.ValueOf(elem.OnFinalize()).Pointer())

	var err error
	SetPanicFn(func(e error) {
		err = e
	})
	defer ResetPanicFn()

	elem.IncRef()
	elem.DecRef()
	elem.Finalize()
	assert.Nil(t, err)

	assert.Equal(t, 1, onFinalizeCalls)
}

func TestRefCountFinalizerNil(t *testing.T) {
	elem := &RefCount{}

	assert.Equal(t, (OnFinalize)(nil), elem.OnFinalize())

	finalizerCalls := 0
	elem.SetOnFinalize(OnFinalize(OnFinalizeFn(func() {
		finalizerCalls++
	})))

	assert.NotNil(t, elem.OnFinalize())

	elem.Finalize()

	assert.Equal(t, 1, finalizerCalls)
}

func TestRefCountReadAfterFree(t *testing.T) {
	elem := &RefCount{}

	var err error
	SetPanicFn(func(e error) {
		err = e
	})
	defer ResetPanicFn()

	elem.IncRef()
	elem.DecRef()
	assert.Nil(t, err)

	elem.IncReads()
	assert.Error(t, err)
	assert.Equal(t, "read after free: reads=1, ref=0", err.Error())
}

func TestRefCountReadFinishAfterFree(t *testing.T) {
	elem := &RefCount{}

	var err error
	SetPanicFn(func(e error) {
		err = e
	})
	defer ResetPanicFn()

	elem.IncRef()
	elem.IncReads()
	assert.Equal(t, 1, elem.NumReaders())
	elem.DecRef()
	assert.Nil(t, err)

	elem.DecReads()
	assert.Error(t, err)
	assert.Equal(t, "read finish after free: reads=0, ref=0", err.Error())
}

func TestRefCountWriteAfterFree(t *testing.T) {
	elem := &RefCount{}

	var err error
	SetPanicFn(func(e error) {
		err = e
	})
	defer ResetPanicFn()

	elem.IncRef()
	elem.DecRef()
	assert.Nil(t, err)

	elem.IncWrites()
	assert.Error(t, err)
	assert.Equal(t, "write after free: writes=1, ref=0", err.Error())
}

func TestRefCountDoubleWrite(t *testing.T) {
	elem := &RefCount{}

	var err error
	SetPanicFn(func(e error) {
		err = e
	})
	defer ResetPanicFn()

	elem.IncRef()
	elem.IncWrites()
	assert.Equal(t, 1, elem.NumWriters())

	elem.IncWrites()
	assert.Error(t, err)
	assert.Equal(t, "double write: writes=2, ref=1", err.Error())
}

func TestRefCountWriteFinishAfterFree(t *testing.T) {
	elem := &RefCount{}

	var err error
	SetPanicFn(func(e error) {
		err = e
	})
	defer ResetPanicFn()

	elem.IncRef()
	elem.IncWrites()
	assert.Equal(t, 1, elem.NumWriters())
	elem.DecRef()
	assert.Nil(t, err)

	elem.DecWrites()
	assert.Error(t, err)
	assert.Equal(t, "write finish after free: writes=0, ref=0", err.Error())
}

func TestRefCountDelayFinalizer(t *testing.T) {
	// NB(r): Make sure to reuse elem so that reuse is accounted for.
	elem := &RefCount{}

	tests := []struct {
		numDelay int
	}{
		{
			numDelay: 1,
		},
		{
			numDelay: 2,
		},
		{
			numDelay: 1024,
		},
		{
			numDelay: 4096,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("num_delay=%d", test.numDelay), func(t *testing.T) {
			onFinalizeCalls := int32(0)
			onFinalize := OnFinalize(OnFinalizeFn(func() {
				atomic.AddInt32(&onFinalizeCalls, 1)
			}))

			elem.SetOnFinalize(onFinalize)
			elem.IncRef()
			elem.DecRef()

			delays := make([]xresource.SimpleCloser, 0, test.numDelay)
			for i := 0; i < test.numDelay; i++ {
				delays = append(delays, elem.DelayFinalizer())
			}

			elem.Finalize()
			require.Equal(t, int32(0), atomic.LoadInt32(&onFinalizeCalls))

			var startWaitingWg, startBeginWg, doneWg sync.WaitGroup
			startBeginWg.Add(1)
			for _, delay := range delays {
				delay := delay
				startWaitingWg.Add(1)
				doneWg.Add(1)
				go func() {
					startWaitingWg.Done()
					startBeginWg.Wait()
					delay.Close()
					doneWg.Done()
				}()
			}

			startWaitingWg.Wait() // Wait for ready to go.
			require.Equal(t, int32(0), atomic.LoadInt32(&onFinalizeCalls))

			startBeginWg.Done() // Open flood gate.
			doneWg.Wait()       // Wait for all done.

			require.Equal(t, int32(1), atomic.LoadInt32(&onFinalizeCalls))
		})
	}
}

func TestRefCountDelayFinalizerDoesNotFinalizeUntilDone(t *testing.T) {
	elem := &RefCount{}

	onFinalizeCalls := int32(0)
	onFinalize := OnFinalize(OnFinalizeFn(func() {
		atomic.AddInt32(&onFinalizeCalls, 1)
	}))

	elem.SetOnFinalize(onFinalize)

	// Delay finalization and complete immediately, should not cause finalization.
	delay := elem.DelayFinalizer()
	delay.Close()

	require.Equal(t, int32(0), atomic.LoadInt32(&onFinalizeCalls))

	elem.Finalize()
	require.Equal(t, int32(1), atomic.LoadInt32(&onFinalizeCalls))
}

func TestRefCountDelayFinalizerPropTest(t *testing.T) {
	var (
		parameters = gopter.DefaultTestParameters()
		seed       = time.Now().UnixNano()
		props      = gopter.NewProperties(parameters)
		reporter   = gopter.NewFormatedReporter(true, 160, os.Stdout)
	)
	parameters.MinSuccessfulTests = 1024
	parameters.Rng.Seed(seed)

	type testInput struct {
		numEvents                 int
		finalizeBeforeDuringAfter int
		finalizeDuringIndex       int
	}

	genTestInput := func() gopter.Gen {
		return gen.
			IntRange(1, 64).
			FlatMap(func(input interface{}) gopter.Gen {
				numEvents := input.(int)
				return gopter.CombineGens(
					gen.IntRange(0, 1),
					gen.IntRange(0, numEvents-1),
				).Map(func(input []interface{}) testInput {
					finalizeBeforeDuringAfter := input[0].(int)
					finalizeDuringIndex := input[1].(int)
					return testInput{
						numEvents:                 numEvents,
						finalizeBeforeDuringAfter: finalizeBeforeDuringAfter,
						finalizeDuringIndex:       finalizeDuringIndex,
					}
				})
			}, reflect.TypeOf(testInput{}))
	}

	// Use single ref count to make sure can be safely reused.
	elem := &RefCount{}

	props.Property("should finalize always once",
		prop.ForAll(func(input testInput) (bool, error) {
			onFinalizeCalls := int32(0)
			onFinalize := OnFinalize(OnFinalizeFn(func() {
				if n := atomic.AddInt32(&onFinalizeCalls, 1); n > 1 {
					panic(fmt.Sprintf("called finalizer more than once: %v", n))
				}
			}))
			elem.SetOnFinalize(onFinalize)

			var startWaitingWg, startBeginWg, startDoneWg, continueWg, doneWg sync.WaitGroup
			startBeginWg.Add(1)
			continueWg.Add(1)
			startWaitingWg.Add(input.numEvents)
			startDoneWg.Add(input.numEvents)
			doneWg.Add(input.numEvents)
			for j := 0; j < input.numEvents; j++ {
				j := j // Capture for lambda
				go func() {
					startWaitingWg.Done()
					startBeginWg.Wait()

					delay := elem.DelayFinalizer()

					// Wait for delayed finalize calls done, cannot call
					// finalize before all delay finalize calls have been issued.
					startDoneWg.Done()
					continueWg.Wait()

					if input.finalizeBeforeDuringAfter == 0 && j == input.finalizeDuringIndex {
						elem.Finalize() // Trigger after an element has began delayed close
					}

					delay.Close()

					if input.finalizeBeforeDuringAfter == 1 && j == input.finalizeDuringIndex {
						elem.Finalize() // Trigger after an element has finished delayed closed
					}

					doneWg.Done()
				}()
			}

			startWaitingWg.Wait() // Wait for ready to go.
			startBeginWg.Done()   // Open flood gate.
			startDoneWg.Wait()    // Wait for delay finalize to be called.
			continueWg.Done()     // Continue the close calls.
			doneWg.Wait()         // Wait for all done.

			if v := atomic.LoadInt32(&onFinalizeCalls); v != 1 {
				return false, fmt.Errorf(
					"finalizer should have been called once, instead: v=%v", v)
			}

			return true, nil
		}, genTestInput()))

	if !props.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func TestLeakDetection(t *testing.T) {
	EnableLeakDetection()
	defer DisableLeakDetection()

	{
		v := &RefCount{}
		v.TrackObject(v)
		v.IncRef()
	}

	runtime.GC()

	var l []string

	for ; len(l) == 0; l = DumpLeaks() {
		// Finalizers are run in a separate goroutine, so we have to wait
		// a little bit here.
		time.Sleep(100 * time.Millisecond)
	}

	assert.NotEmpty(t, l)
}

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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
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

	finalizerCalls := 0
	finalizer := Finalizer(FinalizerFn(func() {
		finalizerCalls++
	}))
	elem.SetFinalizer(finalizer)
	assert.Equal(t,
		reflect.ValueOf(finalizer).Pointer(),
		reflect.ValueOf(elem.Finalizer()).Pointer())

	var err error
	SetPanicFn(func(e error) {
		err = e
	})
	defer ResetPanicFn()

	elem.IncRef()
	elem.DecRef()
	elem.Finalize()
	assert.Nil(t, err)

	assert.Equal(t, 1, finalizerCalls)
}

func TestRefCountFinalizerNil(t *testing.T) {
	elem := &RefCount{}

	assert.Equal(t, (Finalizer)(nil), elem.Finalizer())

	finalizerCalls := 0
	elem.SetFinalizer(Finalizer(FinalizerFn(func() {
		finalizerCalls++
	})))

	assert.NotNil(t, elem.Finalizer())

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

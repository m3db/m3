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

package msgpack

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	msgpack "gopkg.in/vmihailenco/msgpack.v2"
)

var (
	emptyReader *bytes.Buffer
)

// baseIterator is the base iterator that provides common decoding APIs
type baseIterator struct {
	decoder   *msgpack.Decoder // internal decoder that does the actual decoding
	decodeErr error            // error encountered during decoding
}

func newBaseIterator(reader io.Reader) iteratorBase {
	return &baseIterator{
		decoder: msgpack.NewDecoder(reader),
	}
}

// NB(xichen): if reader is not a bufio.Reader or a bytes.Buffer,
// the underlying msgpack decoder creates a bufio.Reader wrapping
// the reader every time Reset() is called
func (it *baseIterator) reset(reader io.Reader) {
	it.decoder.Reset(reader)
	it.decodeErr = nil
}

func (it *baseIterator) err() error       { return it.decodeErr }
func (it *baseIterator) setErr(err error) { it.decodeErr = err }

func (it *baseIterator) decodePolicy() policy.Policy {
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForType(policyType)
	if !ok {
		return policy.Policy{}
	}
	resolution := it.decodeResolution()
	retention := it.decodeRetention()
	p := policy.Policy{Resolution: resolution, Retention: retention}
	it.skip(numActualFields - numExpectedFields)
	return p
}

func (it *baseIterator) decodeResolution() policy.Resolution {
	numActualFields := it.decodeNumObjectFields()
	resolutionType := it.decodeObjectType()
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForTypeWithActual(
		resolutionType,
		numActualFields,
	)
	if !ok {
		return policy.EmptyResolution
	}
	switch resolutionType {
	case knownResolutionType:
		resolutionValue := policy.ResolutionValue(it.decodeVarint())
		if !resolutionValue.IsValid() {
			it.decodeErr = fmt.Errorf("invalid resolution value %v", resolutionValue)
			return policy.EmptyResolution
		}
		it.skip(numActualFields - numExpectedFields)
		if it.decodeErr != nil {
			return policy.EmptyResolution
		}
		resolution, err := resolutionValue.Resolution()
		it.decodeErr = err
		return resolution
	case unknownResolutionType:
		window := time.Duration(it.decodeVarint())
		precision := xtime.Unit(it.decodeVarint())
		if it.decodeErr != nil {
			return policy.EmptyResolution
		}
		if !precision.IsValid() {
			it.decodeErr = fmt.Errorf("invalid precision %v", precision)
			return policy.EmptyResolution
		}
		it.skip(numActualFields - numExpectedFields)
		return policy.Resolution{Window: window, Precision: precision}
	default:
		it.decodeErr = fmt.Errorf("unrecognized resolution type %v", resolutionType)
		return policy.EmptyResolution
	}
}

func (it *baseIterator) decodeRetention() policy.Retention {
	numActualFields := it.decodeNumObjectFields()
	retentionType := it.decodeObjectType()
	numExpectedFields, numActualFields, ok := it.checkNumFieldsForTypeWithActual(
		retentionType,
		numActualFields,
	)
	if !ok {
		return policy.EmptyRetention
	}
	switch retentionType {
	case knownRetentionType:
		retentionValue := policy.RetentionValue(it.decodeVarint())
		if !retentionValue.IsValid() {
			it.decodeErr = fmt.Errorf("invalid retention value %v", retentionValue)
			return policy.EmptyRetention
		}
		it.skip(numActualFields - numExpectedFields)
		if it.decodeErr != nil {
			return policy.EmptyRetention
		}
		retention, err := retentionValue.Retention()
		it.decodeErr = err
		return retention
	case unknownRetentionType:
		retention := policy.Retention(it.decodeVarint())
		it.skip(numActualFields - numExpectedFields)
		return retention
	default:
		it.decodeErr = fmt.Errorf("unrecognized retention type %v", retentionType)
		return policy.EmptyRetention
	}
}

func (it *baseIterator) decodeVersion() int {
	return int(it.decodeVarint())
}

func (it *baseIterator) decodeObjectType() objectType {
	return objectType(it.decodeVarint())
}

func (it *baseIterator) decodeNumObjectFields() int {
	return int(it.decodeArrayLen())
}

func (it *baseIterator) decodeID() metric.ID {
	return metric.ID(it.decodeBytes())
}

func (it *baseIterator) decodeTime() time.Time {
	if it.decodeErr != nil {
		return time.Time{}
	}
	value, err := it.decoder.DecodeTime()
	it.decodeErr = err
	return value
}

// NB(xichen): the underlying msgpack decoder implementation
// always decodes an int64 and looks at the actual decoded
// value to determine the width of the integer (a.k.a. varint
// decoding)
func (it *baseIterator) decodeVarint() int64 {
	if it.decodeErr != nil {
		return 0
	}
	value, err := it.decoder.DecodeInt64()
	it.decodeErr = err
	return value
}

func (it *baseIterator) decodeFloat64() float64 {
	if it.decodeErr != nil {
		return 0.0
	}
	value, err := it.decoder.DecodeFloat64()
	it.decodeErr = err
	return value
}

func (it *baseIterator) decodeBytes() []byte {
	if it.decodeErr != nil {
		return nil
	}
	value, err := it.decoder.DecodeBytes()
	it.decodeErr = err
	return value
}

func (it *baseIterator) decodeBytesLen() int {
	if it.decodeErr != nil {
		return 0
	}
	bytesLen, err := it.decoder.DecodeBytesLen()
	it.decodeErr = err
	return bytesLen
}

func (it *baseIterator) decodeArrayLen() int {
	if it.decodeErr != nil {
		return 0
	}
	value, err := it.decoder.DecodeArrayLen()
	it.decodeErr = err
	return value
}

func (it *baseIterator) skip(numFields int) {
	if it.decodeErr != nil {
		return
	}
	if numFields < 0 {
		it.decodeErr = fmt.Errorf("number of fields to skip is %d", numFields)
		return
	}
	// Otherwise we skip any unexpected extra fields
	for i := 0; i < numFields; i++ {
		if err := it.decoder.Skip(); err != nil {
			it.decodeErr = err
			return
		}
	}
}

func (it *baseIterator) checkNumFieldsForType(objType objectType) (int, int, bool) {
	numActualFields := it.decodeNumObjectFields()
	return it.checkNumFieldsForTypeWithActual(objType, numActualFields)
}

func (it *baseIterator) checkNumFieldsForTypeWithActual(
	objType objectType,
	numActualFields int,
) (int, int, bool) {
	if it.decodeErr != nil {
		return 0, 0, false
	}
	numExpectedFields := numFieldsForType(objType)
	if numExpectedFields > numActualFields {
		it.decodeErr = fmt.Errorf("number of fields mismatch: expected %d actual %d", numExpectedFields, numActualFields)
		return 0, 0, false
	}
	return numExpectedFields, numActualFields, true
}

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
	"fmt"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
)

type encodeVarintFn func(value int64)
type encodeFloat64Fn func(value float64)
type encodeBytesFn func(value []byte)
type encodeArrayLenFn func(value int)

// rawEncoder uses MessagePack for encoding metrics. It is NOT thread-safe.
type rawEncoder struct {
	encoderPool BufferedEncoderPool // pool for internal encoders
	encoder     BufferedEncoder     // internal encoder that does the actual encoding
	err         error               // error encountered during encoding

	encodeVarintFn   encodeVarintFn
	encodeFloat64Fn  encodeFloat64Fn
	encodeBytesFn    encodeBytesFn
	encodeArrayLenFn encodeArrayLenFn
}

// NewRawEncoder creates a new raw encoder
func NewRawEncoder(encoder BufferedEncoder) (RawEncoder, error) {
	enc := &rawEncoder{encoder: encoder}

	enc.encodeVarintFn = enc.encodeVarint
	enc.encodeFloat64Fn = enc.encodeFloat64
	enc.encodeBytesFn = enc.encodeBytes
	enc.encodeArrayLenFn = enc.encodeArrayLen

	return enc, nil
}

func (enc *rawEncoder) Encoder() BufferedEncoder {
	return enc.encoder
}

func (enc *rawEncoder) Reset(encoder BufferedEncoder) {
	enc.encoder = encoder
}

func (enc *rawEncoder) Encode(m *metric.RawMetric, p policy.VersionedPolicies) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeVersion(supportedVersion)
	enc.encodeType(m.Type)
	enc.encodeID(m.ID)
	switch m.Type {
	case metric.CounterType:
		enc.encodeCounterValue(m.CounterVal)
	case metric.BatchTimerType:
		enc.encodeBatchTimerValue(m.BatchTimerVal)
	case metric.GaugeType:
		enc.encodeGaugeValue(m.GaugeVal)
	default:
		enc.err = fmt.Errorf("unrecognized metric type %v", m.Type)
		return enc.err
	}
	enc.encodeVersionedPolicies(p)
	return enc.err
}

func (enc *rawEncoder) encodeCounterValue(value int64) {
	enc.encodeVarintFn(value)
}

func (enc *rawEncoder) encodeBatchTimerValue(values []float64) {
	enc.encodeArrayLenFn(len(values))
	for _, v := range values {
		enc.encodeFloat64Fn(v)
	}
}

func (enc *rawEncoder) encodeGaugeValue(value float64) {
	enc.encodeFloat64Fn(value)
}

func (enc *rawEncoder) encodeVersionedPolicies(p policy.VersionedPolicies) {
	enc.encodeVersion(p.Version)
	// NB(xichen): if this is a default policy, we only encode the policy version
	// and not the actual policies to optimize for the common case where the policies
	// are the default ones
	if p.Version == policy.DefaultPolicyVersion {
		return
	}
	enc.encodeArrayLenFn(len(p.Policies))
	for _, policy := range p.Policies {
		enc.encodePolicy(policy)
	}
}

func (enc *rawEncoder) encodeVersion(version int) {
	enc.encodeVarintFn(int64(version))
}

func (enc *rawEncoder) encodeType(typ metric.Type) {
	enc.encodeVarintFn(int64(typ))
}

func (enc *rawEncoder) encodeID(id metric.IDType) {
	enc.encodeBytesFn([]byte(id))
}

func (enc *rawEncoder) encodePolicy(p policy.Policy) {
	enc.encodeResolution(p.Resolution)
	enc.encodeRetention(p.Retention)
}

func (enc *rawEncoder) encodeResolution(resolution policy.Resolution) {
	if enc.err != nil {
		return
	}
	resolutionValue, err := policy.ValueFromResolution(resolution)
	if err != nil {
		enc.err = err
		return
	}
	enc.encodeVarintFn(int64(resolutionValue))
}

func (enc *rawEncoder) encodeRetention(retention policy.Retention) {
	if enc.err != nil {
		return
	}
	retentionValue, err := policy.ValueFromRetention(retention)
	if err != nil {
		enc.err = err
		return
	}
	enc.encodeVarintFn(int64(retentionValue))
}

// NB(xichen): the underlying msgpack encoder implementation
// always cast an integer value to an int64 and encodes integer
// values as varints, regardless of the actual integer type
func (enc *rawEncoder) encodeVarint(value int64) {
	if enc.err != nil {
		return
	}
	enc.err = enc.encoder.EncodeInt64(value)
}

func (enc *rawEncoder) encodeFloat64(value float64) {
	if enc.err != nil {
		return
	}
	enc.err = enc.encoder.EncodeFloat64(value)
}

func (enc *rawEncoder) encodeBytes(value []byte) {
	if enc.err != nil {
		return
	}
	enc.err = enc.encoder.EncodeBytes(value)
}

func (enc *rawEncoder) encodeArrayLen(value int) {
	if enc.err != nil {
		return
	}
	enc.err = enc.encoder.EncodeArrayLen(value)
}

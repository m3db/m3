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
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
)

type encodeVarintFn func(value int64)
type encodeFloat64Fn func(value float64)
type encodeBytesFn func(value []byte)
type encodeArrayLenFn func(value int)

// multiTypedEncoder uses MessagePack for encoding different types of metrics.
// NB(xichen): It is NOT thread-safe.
type multiTypedEncoder struct {
	encoderPool BufferedEncoderPool // pool for internal encoders
	encoder     BufferedEncoder     // internal encoder that does the actual encoding
	err         error               // error encountered during encoding

	encodeVarintFn   encodeVarintFn
	encodeFloat64Fn  encodeFloat64Fn
	encodeBytesFn    encodeBytesFn
	encodeArrayLenFn encodeArrayLenFn
}

// NewMultiTypedEncoder creates a new multi-typed encoder
func NewMultiTypedEncoder(encoder BufferedEncoder) (MultiTypedEncoder, error) {
	enc := &multiTypedEncoder{encoder: encoder}

	enc.encodeVarintFn = enc.encodeVarint
	enc.encodeFloat64Fn = enc.encodeFloat64
	enc.encodeBytesFn = enc.encodeBytes
	enc.encodeArrayLenFn = enc.encodeArrayLen

	return enc, nil
}

func (enc *multiTypedEncoder) Encoder() BufferedEncoder {
	return enc.encoder
}

func (enc *multiTypedEncoder) Reset(encoder BufferedEncoder) {
	enc.encoder = encoder
}

func (enc *multiTypedEncoder) EncodeCounter(c metric.Counter, p policy.VersionedPolicies) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeVersion(supportedVersion)
	enc.encodeType(metric.CounterType)
	enc.encodeID(c.ID)
	enc.encodeVarintFn(int64(c.Value))
	enc.encodeVersionedPolicies(p)
	return enc.err
}

func (enc *multiTypedEncoder) EncodeBatchTimer(bt metric.BatchTimer, p policy.VersionedPolicies) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeVersion(supportedVersion)
	enc.encodeType(metric.BatchTimerType)
	enc.encodeID(bt.ID)
	enc.encodeArrayLenFn(len(bt.Values))
	for _, v := range bt.Values {
		enc.encodeFloat64Fn(v)
	}
	enc.encodeVersionedPolicies(p)
	return enc.err
}

func (enc *multiTypedEncoder) EncodeGauge(g metric.Gauge, p policy.VersionedPolicies) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeVersion(supportedVersion)
	enc.encodeType(metric.GaugeType)
	enc.encodeID(g.ID)
	enc.encodeFloat64Fn(g.Value)
	enc.encodeVersionedPolicies(p)
	return enc.err
}

func (enc *multiTypedEncoder) encodeVersionedPolicies(p policy.VersionedPolicies) {
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

func (enc *multiTypedEncoder) encodeVersion(version int) {
	enc.encodeVarintFn(int64(version))
}

func (enc *multiTypedEncoder) encodeType(typ metric.Type) {
	enc.encodeVarintFn(int64(typ))
}

func (enc *multiTypedEncoder) encodeID(id metric.IDType) {
	enc.encodeBytesFn([]byte(id))
}

func (enc *multiTypedEncoder) encodePolicy(p policy.Policy) {
	enc.encodeResolution(p.Resolution)
	enc.encodeRetention(p.Retention)
}

func (enc *multiTypedEncoder) encodeResolution(resolution policy.Resolution) {
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

func (enc *multiTypedEncoder) encodeRetention(retention policy.Retention) {
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
func (enc *multiTypedEncoder) encodeVarint(value int64) {
	if enc.err != nil {
		return
	}
	enc.err = enc.encoder.EncodeInt64(value)
}

func (enc *multiTypedEncoder) encodeFloat64(value float64) {
	if enc.err != nil {
		return
	}
	enc.err = enc.encoder.EncodeFloat64(value)
}

func (enc *multiTypedEncoder) encodeBytes(value []byte) {
	if enc.err != nil {
		return
	}
	enc.err = enc.encoder.EncodeBytes(value)
}

func (enc *multiTypedEncoder) encodeArrayLen(value int) {
	if enc.err != nil {
		return
	}
	enc.err = enc.encoder.EncodeArrayLen(value)
}

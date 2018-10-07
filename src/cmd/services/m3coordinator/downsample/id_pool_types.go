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

package downsample

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/m3db/m3/src/dbnode/serialize"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"

	"github.com/prometheus/common/model"
)

var (
	defaultMetricNameTagName = []byte(model.MetricNameLabel)
	rollupTagName            = []byte("m3_rollup")
	rollupTagValue           = []byte("true")
	rollupTag                = ident.Tag{
		Name:  ident.BytesID(rollupTagName),
		Value: ident.BytesID(rollupTagValue),
	}

	errNoMetricNameTag = errors.New("no metric name tag found")
)

func isRollupID(
	sortedTagPairs []byte,
	iteratorPool *xserialize.EncodedTagsIteratorPool,
) bool {
	iter := iteratorPool.Get()
	iter.Reset(sortedTagPairs)

	tagValue, ok := iter.TagValue(rollupTagName)
	isRollupID := ok && bytes.Equal(tagValue, rollupTagValue)
	iter.Close()

	return isRollupID
}

// rollupIDProvider is a constructor for rollup IDs, it can be pooled to avoid
// requiring allocation every time we need to construct a rollup ID.
// When used as a ident.TagIterator for the call to serialize.TagEncoder Encode
// method, it will return the rollup tag in the correct alphabetical order
// when progressing through the existing tags.
type rollupIDProvider struct {
	index          int
	tagPairs       []id.TagPair
	rollupTagIndex int

	tagEncoder serialize.TagEncoder
	pool       *rollupIDProviderPool
}

func newRollupIDProvider(
	tagEncoder serialize.TagEncoder,
	pool *rollupIDProviderPool,
) *rollupIDProvider {
	return &rollupIDProvider{
		tagEncoder: tagEncoder,
		pool:       pool,
	}
}

func (p *rollupIDProvider) provide(
	tagPairs []id.TagPair,
) ([]byte, error) {
	p.reset(tagPairs)
	p.tagEncoder.Reset()
	if err := p.tagEncoder.Encode(p); err != nil {
		return nil, err
	}
	data, ok := p.tagEncoder.Data()
	if !ok {
		return nil, fmt.Errorf("unable to access encoded tags: ok=%v", ok)
	}
	// Need to return a copy
	id := append([]byte(nil), data.Bytes()...)
	// Reset after computing
	p.reset(nil)
	return id, nil
}

func (p *rollupIDProvider) reset(
	tagPairs []id.TagPair,
) {
	p.index = -1
	p.tagPairs = tagPairs
	p.rollupTagIndex = -1
	for idx, pair := range tagPairs {
		if bytes.Compare(rollupTagName, pair.Name) < 0 {
			p.rollupTagIndex = idx
			break
		}
	}
	if p.rollupTagIndex == -1 {
		p.rollupTagIndex = len(p.tagPairs)
	}
}

func (p *rollupIDProvider) finalize() {
	if p.pool != nil {
		p.pool.Put(p)
	}
}

func (p *rollupIDProvider) Next() bool {
	p.index++
	return p.index < p.Len()
}

func (p *rollupIDProvider) CurrentIndex() int {
	if p.index >= 0 {
		return p.index
	}
	return 0
}

func (p *rollupIDProvider) Current() ident.Tag {
	idx := p.index
	if idx == p.rollupTagIndex {
		return rollupTag
	}

	if idx > p.rollupTagIndex {
		// Effective index is subtracted by 1
		idx--
	}

	return ident.Tag{
		Name:  ident.BytesID(p.tagPairs[idx].Name),
		Value: ident.BytesID(p.tagPairs[idx].Value),
	}
}

func (p *rollupIDProvider) Err() error {
	return nil
}

func (p *rollupIDProvider) Close() {
	// No-op
}

func (p *rollupIDProvider) Len() int {
	return len(p.tagPairs) + 1
}

func (p *rollupIDProvider) Remaining() int {
	return p.Len() - p.index - 1
}

func (p *rollupIDProvider) Duplicate() ident.TagIterator {
	duplicate := p.pool.Get()
	duplicate.reset(p.tagPairs)
	return duplicate
}

type rollupIDProviderPool struct {
	tagEncoderPool serialize.TagEncoderPool
	pool           pool.ObjectPool
}

func newRollupIDProviderPool(
	tagEncoderPool serialize.TagEncoderPool,
	opts pool.ObjectPoolOptions,
) *rollupIDProviderPool {
	return &rollupIDProviderPool{
		tagEncoderPool: tagEncoderPool,
		pool:           pool.NewObjectPool(opts),
	}
}

func (p *rollupIDProviderPool) Init() {
	p.pool.Init(func() interface{} {
		return newRollupIDProvider(p.tagEncoderPool.Get(), p)
	})
}

func (p *rollupIDProviderPool) Get() *rollupIDProvider {
	return p.pool.Get().(*rollupIDProvider)
}

func (p *rollupIDProviderPool) Put(v *rollupIDProvider) {
	p.pool.Put(v)
}

func resolveEncodedTagsNameTag(
	id []byte,
	iterPool *xserialize.EncodedTagsIteratorPool,
	nameTag []byte,
) ([]byte, error) {
	// ID is always the encoded tags for downsampling IDs
	iter := iterPool.Get()
	iter.Reset(id)
	defer iter.Close()

	value, ok := iter.TagValue(nameTag)
	if !ok {
		// No name was found in encoded tags
		return nil, errNoMetricNameTag
	}

	idx := bytes.Index(id, value)
	if idx == -1 {
		return nil, fmt.Errorf(
			"resolved metric name tag value not found in ID: %v", value)
	}

	// Return original reference to avoid needing to return a copy
	return id[idx : idx+len(value)], nil
}

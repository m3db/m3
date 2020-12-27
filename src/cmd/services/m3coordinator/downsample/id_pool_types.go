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

	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"

	"github.com/prometheus/common/model"
)

var (
	defaultMetricNameTagName = []byte(model.MetricNameLabel)
	rollupTagName            = []byte("__rollup__")
	rollupTagValue           = []byte("true")
	rollupTag                = ident.Tag{
		Name:  ident.BytesID(rollupTagName),
		Value: ident.BytesID(rollupTagValue),
	}

	errNoMetricNameTag = errors.New("no metric name tag found")
)

func isRollupID(
	sortedTagPairs []byte,
	iteratorPool serialize.MetricTagsIteratorPool,
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
	newName        []byte
	tagPairs       []id.TagPair
	nameTagIndex   int
	rollupTagIndex int

	tagEncoder             serialize.TagEncoder
	pool                   *rollupIDProviderPool
	nameTag                ident.ID
	nameTagBytes           []byte
	nameTagBeforeRollupTag bool
	tagNameID              *ident.ReusableBytesID
	tagValueID             *ident.ReusableBytesID
}

func newRollupIDProvider(
	tagEncoder serialize.TagEncoder,
	pool *rollupIDProviderPool,
	nameTag ident.ID,
) *rollupIDProvider {
	nameTagBytes := nameTag.Bytes()
	nameTagBeforeRollupTag := bytes.Compare(nameTagBytes, rollupTagName) < 0
	return &rollupIDProvider{
		tagEncoder:             tagEncoder,
		pool:                   pool,
		nameTag:                nameTag,
		nameTagBytes:           nameTagBytes,
		nameTagBeforeRollupTag: nameTagBeforeRollupTag,
		tagNameID:              ident.NewReusableBytesID(),
		tagValueID:             ident.NewReusableBytesID(),
	}
}

func (p *rollupIDProvider) provide(
	newName []byte,
	tagPairs []id.TagPair,
) ([]byte, error) {
	p.reset(newName, tagPairs)
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
	p.reset(nil, nil)
	return id, nil
}

func (p *rollupIDProvider) reset(
	newName []byte,
	tagPairs []id.TagPair,
) {
	p.index = -1
	p.newName = newName
	p.tagPairs = tagPairs
	p.nameTagIndex = -1
	p.rollupTagIndex = -1
	for idx, pair := range tagPairs {
		if p.nameTagIndex == -1 && bytes.Compare(p.nameTagBytes, pair.Name) < 0 {
			p.nameTagIndex = idx
		}
		if p.rollupTagIndex == -1 && bytes.Compare(rollupTagName, pair.Name) < 0 {
			p.rollupTagIndex = idx
		}
	}

	if p.nameTagIndex == p.rollupTagIndex {
		if p.nameTagBeforeRollupTag {
			p.rollupTagIndex++
		} else {
			p.nameTagIndex++
		}
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
	if idx == p.nameTagIndex {
		p.tagValueID.Reset(p.newName)
		return ident.Tag{
			Name:  p.nameTag,
			Value: p.tagValueID,
		}
	}
	if idx == p.rollupTagIndex {
		return rollupTag
	}

	if p.index > p.nameTagIndex {
		// Effective index is subtracted by 1
		idx--
	}
	if p.index > p.rollupTagIndex {
		// Effective index is subtracted by 1
		idx--
	}

	p.tagNameID.Reset(p.tagPairs[idx].Name)
	p.tagValueID.Reset(p.tagPairs[idx].Value)
	return ident.Tag{
		Name:  p.tagNameID,
		Value: p.tagValueID,
	}
}

func (p *rollupIDProvider) Err() error {
	return nil
}

func (p *rollupIDProvider) Close() {}

func (p *rollupIDProvider) Len() int {
	return len(p.tagPairs) + 2
}

func (p *rollupIDProvider) Remaining() int {
	return p.Len() - p.index - 1
}

func (p *rollupIDProvider) Duplicate() ident.TagIterator {
	duplicate := p.pool.Get()
	duplicate.reset(p.newName, p.tagPairs)
	return duplicate
}

func (p *rollupIDProvider) Rewind() {
	p.index = -1
}

type rollupIDProviderPool struct {
	tagEncoderPool serialize.TagEncoderPool
	pool           pool.ObjectPool
	nameTag        ident.ID
}

func newRollupIDProviderPool(
	tagEncoderPool serialize.TagEncoderPool,
	opts pool.ObjectPoolOptions,
	nameTag ident.ID,
) *rollupIDProviderPool {
	return &rollupIDProviderPool{
		tagEncoderPool: tagEncoderPool,
		pool:           pool.NewObjectPool(opts),
		nameTag:        nameTag,
	}
}

func (p *rollupIDProviderPool) Init() {
	p.pool.Init(func() interface{} {
		return newRollupIDProvider(p.tagEncoderPool.Get(), p, p.nameTag)
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
	nameTag []byte,
) ([]byte, error) {
	value, ok, err := serialize.TagValueFromEncodedTagsFast(id, nameTag)
	if err != nil {
		return nil, err
	}
	if !ok {
		// No name was found in encoded tags.
		return nil, errNoMetricNameTag
	}

	// Return original reference to avoid needing to return a copy.
	return value, nil
}

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

	coordmodel "github.com/m3db/m3/src/cmd/services/m3coordinator/model"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
)

var (
	rollupTagName      = []byte(coordmodel.RollupTagName)
	rollupTagValue     = []byte(coordmodel.RollupTagValue)
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
	index        int
	len          int
	mergeTagsIdx int
	newName      []byte
	tagPairs     []id.TagPair
	curr         id.TagPair
	currIdx      int

	tagEncoder   serialize.TagEncoder
	pool         *rollupIDProviderPool
	nameTag      ident.ID
	nameTagBytes []byte
	tagNameID    *ident.ReusableBytesID
	tagValueID   *ident.ReusableBytesID
	mergeTags    []id.TagPair
}

func newRollupIDProvider(
	tagEncoder serialize.TagEncoder,
	pool *rollupIDProviderPool,
	nameTag ident.ID,
) *rollupIDProvider {
	nameTagBytes := nameTag.Bytes()
	nameTagBeforeRollupTag := bytes.Compare(nameTagBytes, rollupTagName) < 0
	mergeTags := []id.TagPair{
		{
			Name:  rollupTagName,
			Value: rollupTagValue,
		},
		{
			Name: nameTagBytes,
			// Value is set in reset
		},
	}
	if nameTagBeforeRollupTag {
		mergeTags[0], mergeTags[1] = mergeTags[1], mergeTags[0]
	}
	return &rollupIDProvider{
		tagEncoder:   tagEncoder,
		pool:         pool,
		nameTag:      nameTag,
		nameTagBytes: nameTagBytes,
		tagNameID:    ident.NewReusableBytesID(),
		tagValueID:   ident.NewReusableBytesID(),
		mergeTags:    mergeTags,
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
	p.newName = newName
	p.tagPairs = tagPairs
	p.Rewind()

	var dups int
	// precompute the length of the "combined" slice for the Len() method.
	// mergeTags is small so it's fine to do n^2 instead of a more complicated O(n) merge scan.
	for j := range p.mergeTags {
		// update the name tag as well.
		if bytes.Equal(p.mergeTags[j].Name, p.nameTagBytes) {
			p.mergeTags[j].Value = newName
		}
		for i := range p.tagPairs {
			if bytes.Equal(p.tagPairs[i].Name, p.mergeTags[j].Name) {
				dups++
			}
		}
	}
	p.len = len(p.tagPairs) + len(p.mergeTags) - dups
}

func (p *rollupIDProvider) finalize() {
	if p.pool != nil {
		p.pool.Put(p)
	}
}

// Next takes the smallest element across both sets of tags, removing any duplicates between the lists.
func (p *rollupIDProvider) Next() bool {
	if p.index == len(p.tagPairs) && p.mergeTagsIdx == len(p.mergeTags) {
		// at the end of both sets
		return false
	}
	switch {
	case p.index == len(p.tagPairs):
		// only merged tags left
		p.curr = p.mergeTags[p.mergeTagsIdx]
		p.mergeTagsIdx++
	case p.mergeTagsIdx == len(p.mergeTags):
		// only provided tags left
		p.curr = p.tagPairs[p.index]
		p.index++
	case bytes.Equal(p.tagPairs[p.index].Name, p.mergeTags[p.mergeTagsIdx].Name):
		// a merge tag exists in the provided tag, advance both to prevent duplicates.
		p.curr = p.tagPairs[p.index]
		p.index++
		p.mergeTagsIdx++
	case bytes.Compare(p.tagPairs[p.index].Name, p.mergeTags[p.mergeTagsIdx].Name) < 0:
		// the next provided tag is less
		p.curr = p.tagPairs[p.index]
		p.index++
	default:
		// the next merge tag is less
		p.curr = p.mergeTags[p.mergeTagsIdx]
		p.mergeTagsIdx++
	}
	p.currIdx++
	return true
}

func (p *rollupIDProvider) CurrentIndex() int {
	return p.currIdx
}

func (p *rollupIDProvider) Current() ident.Tag {
	p.tagNameID.Reset(p.curr.Name)
	p.tagValueID.Reset(p.curr.Value)
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
	return p.len
}

func (p *rollupIDProvider) Remaining() int {
	return p.Len() - p.CurrentIndex() - 1
}

func (p *rollupIDProvider) Duplicate() ident.TagIterator {
	duplicate := p.pool.Get()
	duplicate.reset(p.newName, p.tagPairs)
	return duplicate
}

func (p *rollupIDProvider) Rewind() {
	p.index = 0
	p.mergeTagsIdx = 0
	p.currIdx = -1
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

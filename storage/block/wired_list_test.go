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

package block

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3db/runtime"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

// The tests in this file use Start and Stop a lot to ensure
// access to the fields of the list is safe and not-concurrent.

// avoid creating a ton of new test options
var testOptions = NewOptions()

func newTestWiredList(
	overrideRuntimeOpts runtime.Options,
	overrideMetricsScope tally.Scope,
) (*WiredList, runtime.OptionsManager) {
	runopts := runtime.NewOptions()
	if overrideRuntimeOpts != nil {
		runopts = overrideRuntimeOpts
	}
	runtimeOptsMgr := runtime.NewOptionsManager(runopts)
	iopts := instrument.NewOptions()
	if overrideMetricsScope != nil {
		iopts = iopts.SetMetricsScope(overrideMetricsScope)
	}
	return NewWiredList(runtimeOptsMgr, iopts), runtimeOptsMgr
}

func newTestUnwireableBlock(
	ctrl *gomock.Controller,
	name string,
	opts Options,
) *dbBlock {
	segment := ts.Segment{
		Head: checked.NewBytes([]byte(name), nil),
	}
	segment.Head.IncRef()

	bl := NewWiredDatabaseBlock(time.Time{}, segment, opts).(*dbBlock)
	bl.retriever = NewMockDatabaseShardBlockRetriever(ctrl)
	bl.retrieveID = ts.StringID(name)

	return bl
}

func TestWiredListInsertsAndUpdatesWiredBlocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l, _ := newTestWiredList(nil, nil)

	opts := testOptions.SetWiredList(l)

	l.Start()

	var blocks []*dbBlock
	for i := 0; i < 3; i++ {
		bl := newTestUnwireableBlock(ctrl, fmt.Sprintf("foo.%d", i), opts)
		blocks = append(blocks, bl)
	}

	l.update(blocks[0])
	l.update(blocks[1])
	l.update(blocks[2])
	l.update(blocks[1])

	l.Stop()

	// Order due to LRU should be: 0, 2, 1
	assert.Equal(t, blocks[0], l.root.next)
	assert.Equal(t, blocks[2], l.root.next.next)
	assert.Equal(t, blocks[1], l.root.next.next.next)

	// Assert end
	assert.Equal(t, &l.root, l.root.next.next.next.next)

	// Assert tail
	assert.Equal(t, blocks[1], l.root.prev)
}

func TestWiredListRemovesUnwiredBlocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l, _ := newTestWiredList(nil, nil)

	opts := testOptions.SetWiredList(l)

	l.Start()

	var blocks []*dbBlock
	for i := 0; i < 2; i++ {
		bl := newTestUnwireableBlock(ctrl, fmt.Sprintf("foo.%d", i), opts)
		blocks = append(blocks, bl)
	}

	l.update(blocks[0])
	l.update(blocks[1])
	l.update(blocks[0])

	l.Stop()

	// Order due to LRU should be: 1, 0
	assert.Equal(t, blocks[1], l.root.next)
	assert.Equal(t, blocks[0], l.root.next.next)

	// Unwire block and assert removed
	blocks[1].segment = ts.Segment{}

	l.Start()
	l.update(blocks[1])
	l.Stop()

	assert.Equal(t, 1, l.len)
	assert.Equal(t, blocks[0], l.root.next)
	assert.Equal(t, &l.root, l.root.next.next)

	// Unwire final block and assert removed
	blocks[0].segment = ts.Segment{}

	l.Start()
	l.update(blocks[0])
	l.Stop()

	assert.Equal(t, 0, l.len)
	assert.Equal(t, &l.root, l.root.next)
	assert.Equal(t, &l.root, l.root.prev)
}

// wiredListTestWiredBlocksString use for debugging order of wired list
func wiredListTestWiredBlocksString(l *WiredList) string {
	b := bytes.NewBuffer(nil)
	for bl := l.root.next; bl != &l.root; bl = bl.next {
		b.WriteString(fmt.Sprintf("%s\n", string(bl.segment.Head.Get())))
	}
	return b.String()
}

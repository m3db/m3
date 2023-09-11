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

package block

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
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
	runtimeOptsMgr := runtime.NewOptionsManager()
	iopts := instrument.NewOptions()
	if overrideMetricsScope != nil {
		iopts = iopts.SetMetricsScope(overrideMetricsScope)
	}
	copts := clock.NewOptions()
	return NewWiredList(WiredListOptions{
		RuntimeOptionsManager: runtimeOptsMgr,
		InstrumentOptions:     iopts,
		ClockOptions:          copts,
		// Use a small channel to stress-test the implementation
		EventsChannelSize: 1,
	}), runtimeOptsMgr
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

	bl := NewDatabaseBlock(0, 0, segment, opts, namespace.Context{}).(*dbBlock)
	bl.Lock()
	bl.seriesID = ident.StringID(name)
	bl.wasRetrievedFromDisk = true
	bl.Unlock()

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

	l.BlockingUpdate(blocks[0])
	l.BlockingUpdate(blocks[1])
	l.BlockingUpdate(blocks[2])
	l.BlockingUpdate(blocks[1])

	l.Stop()

	// Order due to LRU should be: 0, 2, 1
	require.Equal(t, blocks[0], l.root.next())
	require.Equal(t, blocks[2], l.root.next().next())
	require.Equal(t, blocks[1], l.root.next().next().next())

	// Assert end
	require.Equal(t, &l.root, l.root.next().next().next().next())

	// Assert tail
	require.Equal(t, blocks[1], l.root.prev())
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

	l.BlockingUpdate(blocks[0])
	l.BlockingUpdate(blocks[1])
	l.BlockingUpdate(blocks[0])

	l.Stop()

	// Order due to LRU should be: 1, 0
	require.Equal(t, blocks[1], l.root.next())
	require.Equal(t, blocks[0], l.root.next().next())

	// Unwire block and assert removed
	blocks[1].closed = true

	l.Start()
	l.BlockingUpdate(blocks[1])
	l.Stop()

	require.Equal(t, 1, l.length)
	require.Equal(t, blocks[0], l.root.next())
	require.Equal(t, &l.root, l.root.next().next())

	// Unwire final block and assert removed
	blocks[0].closed = true

	l.Start()
	l.BlockingUpdate(blocks[0])
	l.Stop()

	require.Equal(t, 0, l.length)
	require.Equal(t, &l.root, l.root.next())
	require.Equal(t, &l.root, l.root.prev())
}

// wiredListTestWiredBlocksString is used to debug the order of the wired list
func wiredListTestWiredBlocksString(l *WiredList) string { // nolint: unused
	b := bytes.NewBuffer(nil)
	for bl := l.root.next(); bl != &l.root; bl = bl.next() {
		dbBlock := bl.(*dbBlock)
		b.WriteString(fmt.Sprintf("%s\n", string(dbBlock.segment.Head.Bytes())))
	}
	return b.String()
}

func TestWiredListUpdateNoopsAfterStop(t *testing.T) {
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

	l.BlockingUpdate(blocks[0])
	l.BlockingUpdate(blocks[1])
	require.NoError(t, l.Stop())
	l.BlockingUpdate(blocks[0])
	l.NonBlockingUpdate(blocks[0])

	// Order due to LRU should be: 0, 1, since next updates are rejected
	require.Equal(t, blocks[0], l.root.next())
	require.Equal(t, blocks[1], l.root.next().next())

	// Assert end
	require.Equal(t, &l.root, l.root.next().next().next())

	// Assert tail
	require.Equal(t, blocks[1], l.root.prev())
}

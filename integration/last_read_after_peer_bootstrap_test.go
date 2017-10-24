// +build integration

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

package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/integration/generate"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"
	m3dbtime "github.com/m3db/m3db/x/time"
	xlog "github.com/m3db/m3x/log"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test ensures that only reads against a node causes last read
// for a block to be updated.  Also ensures that bootstrapping from
// a peer does not cause the blocks to look like they've been read,
// there is a clear distinction between FetchBlocks API and a read.
func TestLastReadAfterPeerBootstrap(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setups
	log := xlog.SimpleLogger
	ropts := retention.NewOptions().
		SetRetentionPeriod(8 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(2 * time.Minute)

	namesp, err := namespace.NewMetadata(testNamespaces[0],
		namespace.NewOptions().SetRetentionOptions(ropts))
	require.NoError(t, err)
	opts := newTestOptions(t).
		SetNamespaces([]namespace.Metadata{namesp}).
		SetTickInterval(time.Minute)

	setupOpts := []bootstrappableTestSetupOptions{
		{disablePeersBootstrapper: true},
		{disablePeersBootstrapper: false},
	}
	setups, closeFn := newDefaultBootstrappableTestSetups(t, opts, setupOpts)
	defer closeFn()

	// Write test data for first node
	now := setups[0].getNowFn() // now is already truncate to block size
	blockSize := ropts.BlockSize()
	start, end := now.Add(-3*blockSize), now
	seriesMaps := generate.BlocksByStart([]generate.BlockConfig{
		{[]string{"foo", "bar"}, 50, now.Add(-3 * blockSize)},
		{[]string{"foo", "baz"}, 50, now.Add(-2 * blockSize)},
		{[]string{"foo", "qux"}, 50, now.Add(-1 * blockSize)},
	})
	err = writeTestDataToDisk(namesp, setups[0], seriesMaps)
	require.NoError(t, err)

	// Start the first server with filesystem bootstrapper
	require.NoError(t, setups[0].startServer())

	// Start the last server with peers and filesystem bootstrappers
	require.NoError(t, setups[1].startServer())
	log.Debug("servers are now up")

	// Stop the servers
	defer func() {
		log.Debug("servers shutting down")
		setups.parallel(func(s *testSetup) {
			require.NoError(t, s.stopServer())
		})
		log.Debug("servers are now down")
	}()

	// Verify no reads have occurred
	log.Debug("verifying no blocks seem read yet")
	setups.parallel(func(s *testSetup) {
		verifyLastReads(t, s, namesp.ID(), start, end, lastReadState{})
	})

	// Issue initial reads
	_, err = m3dbClientFetch(setups[0].m3dbClient, &rpc.FetchRequest{
		RangeType:  rpc.TimeType_UNIX_SECONDS,
		RangeStart: start.Unix(),
		RangeEnd:   start.Add(2 * blockSize).Unix(),
		NameSpace:  namesp.ID().String(),
		ID:         "foo",
	})
	assert.NoError(t, err)

	_, err = m3dbClientFetch(setups[1].m3dbClient, &rpc.FetchRequest{
		RangeType:  rpc.TimeType_UNIX_SECONDS,
		RangeStart: start.Unix(),
		RangeEnd:   start.Add(2 * blockSize).Unix(),
		NameSpace:  namesp.ID().String(),
		ID:         "bar",
	})
	assert.NoError(t, err)

	_, err = m3dbClientFetch(setups[1].m3dbClient, &rpc.FetchRequest{
		RangeType:  rpc.TimeType_UNIX_SECONDS,
		RangeStart: start.Add(1 * blockSize).Unix(),
		RangeEnd:   start.Add(3 * blockSize).Unix(),
		NameSpace:  namesp.ID().String(),
		ID:         "baz",
	})
	assert.NoError(t, err)

	// Move time forward a little
	now = now.Add(5 * time.Minute)
	for _, setup := range setups {
		setup.setNowFn(now)
	}

	// Issue subsequent read
	_, err = m3dbClientFetch(setups[0].m3dbClient, &rpc.FetchRequest{
		RangeType:  rpc.TimeType_UNIX_SECONDS,
		RangeStart: start.Add(1 * blockSize).Unix(),
		RangeEnd:   start.Add(2 * blockSize).Unix(),
		NameSpace:  namesp.ID().String(),
		ID:         "foo",
	})
	assert.NoError(t, err)

	// Verify last read times
	log.Debug("verifying initial set of reads")
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		verifyLastReads(t, setups[0], namesp.ID(), start, end, lastReadState{
			ids: map[string]lastReadIDState{
				"foo": {blocks: map[m3dbtime.UnixNano]lastReadBlockState{
					m3dbtime.ToUnixNano(start):                    {lastRead: end},
					m3dbtime.ToUnixNano(start.Add(1 * blockSize)): {lastRead: end.Add(5 * time.Minute)},
				}},
			},
		})
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		verifyLastReads(t, setups[1], namesp.ID(), start, end, lastReadState{
			ids: map[string]lastReadIDState{
				"bar": {blocks: map[m3dbtime.UnixNano]lastReadBlockState{
					m3dbtime.ToUnixNano(start): {lastRead: end},
				}},
				"baz": {blocks: map[m3dbtime.UnixNano]lastReadBlockState{
					m3dbtime.ToUnixNano(start.Add(1 * blockSize)): {lastRead: end},
				}},
			},
		})
	}()
	wg.Wait()

	// Move time forward a little again
	now = now.Add(2 * time.Minute)
	for _, setup := range setups {
		setup.setNowFn(now)
	}

	// Verify in-memory data match what we expect before we finish,
	// also updating last read time of all blocks
	log.Debug("verifying series data matches")
	setups.parallel(func(s *testSetup) {
		verifySeriesMaps(t, s, namesp.ID(), seriesMaps)
	})

	// Verify last read times
	log.Debug("verifying all reads")
	allRead := lastReadState{ids: map[string]lastReadIDState{
		"foo": lastReadIDState{blocks: map[m3dbtime.UnixNano]lastReadBlockState{
			m3dbtime.ToUnixNano(start):                    {lastRead: end.Add(7 * time.Minute)},
			m3dbtime.ToUnixNano(start.Add(1 * blockSize)): {lastRead: end.Add(7 * time.Minute)},
			m3dbtime.ToUnixNano(start.Add(2 * blockSize)): {lastRead: end.Add(7 * time.Minute)},
		}},
		"bar": lastReadIDState{blocks: map[m3dbtime.UnixNano]lastReadBlockState{
			m3dbtime.ToUnixNano(start): {lastRead: end.Add(7 * time.Minute)},
		}},
		"baz": lastReadIDState{blocks: map[m3dbtime.UnixNano]lastReadBlockState{
			m3dbtime.ToUnixNano(start.Add(1 * blockSize)): {lastRead: end.Add(7 * time.Minute)},
		}},
		"qux": lastReadIDState{blocks: map[m3dbtime.UnixNano]lastReadBlockState{
			m3dbtime.ToUnixNano(start.Add(2 * blockSize)): {lastRead: end.Add(7 * time.Minute)},
		}},
	}}

	setups.parallel(func(s *testSetup) {
		verifyLastReads(t, s, namesp.ID(), start, end, allRead)
	})

	log.Debug("done verification")
}

type lastReadState struct {
	ids map[string]lastReadIDState
}

type lastReadIDState struct {
	blocks map[m3dbtime.UnixNano]lastReadBlockState
}

type lastReadBlockState struct {
	lastRead time.Time
}

func verifyLastReads(
	t *testing.T,
	setup *testSetup,
	namespace ts.ID,
	start, end time.Time,
	expected lastReadState,
) {
	metadatas, err := m3dbClientFetchBlocksMetadata(setup.m3dbAdminClient,
		namespace, setup.shardSet.AllIDs(), start, end)
	assert.NoError(t, err)
	if err != nil {
		return
	}

	// Build actual results
	actual := lastReadState{ids: map[string]lastReadIDState{}}
	for _, shardMetadatas := range metadatas {
		for _, metadata := range shardMetadatas {
			if metadata.LastRead.IsZero() {
				// Don't consider metadata where last read is zero
				continue
			}

			assert.Equal(t, setup.hostID, metadata.Host.ID())

			id := metadata.ID.String()
			state := actual.ids[id]
			if state.blocks == nil {
				state.blocks = map[m3dbtime.UnixNano]lastReadBlockState{}
			}

			block := state.blocks[m3dbtime.ToUnixNano(metadata.Start)]
			block.lastRead = metadata.LastRead
			state.blocks[m3dbtime.ToUnixNano(metadata.Start)] = block

			actual.ids[id] = state
		}
	}

	// Compare to expected results
	assert.Equal(t, len(expected.ids), len(actual.ids), fmt.Sprintf(
		"expected results mismatch with actual IDs: expected=%d, actual=%d",
		len(expected.ids), len(actual.ids)))
	for id, expectBlocks := range expected.ids {
		actualBlocks, ok := actual.ids[id]
		assert.True(t, ok, fmt.Sprintf(
			"no actual result for expected ID: id=%s", id))
		if !ok {
			continue
		}

		expectedBlocksLen := len(expectBlocks.blocks)
		actualBlocksLen := len(actualBlocks.blocks)
		assert.Equal(t, expectedBlocksLen, actualBlocksLen, fmt.Sprintf(
			"expected %d blocks instead actual: blocks=%d",
			expectedBlocksLen, actualBlocksLen))

		for start, expectBlock := range expectBlocks.blocks {
			actualBlock, ok := actualBlocks.blocks[start]
			assert.True(t, ok, fmt.Sprintf(
				"no actual result for expected ID at block: id=%s, block=%v",
				id, start))
			if !ok {
				continue
			}

			expectLastRead := expectBlock.lastRead
			actualLastRead := actualBlock.lastRead
			assert.True(t, expectLastRead.Equal(actualLastRead), fmt.Sprintf(
				"last read mismatch for expected ID at block: id=%s, block=%v, "+
					"expectLastRead=%v, actualLastRead=%v",
				id, start, expectLastRead, actualLastRead))
		}
	}
}

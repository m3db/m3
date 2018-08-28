package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/storage/series/lookup"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
)

// TestSeriesWiredListConcurrentInteractions was added as a regression test
// after discovering that interactions between a single series and the wired
// list could trigger a mutual dead lock. Specifically, if the wired list event
// channel was full, then the series could get blocked on a call to list.Update()
// in the OnRetrieveBlockMethod while the only goroutine pulling items off of that
// channel was stuck on the same series OnEvictedFromWiredList method. In that case,
// the OnRetrieveBlockMethod was stuck on a channel send while holding a lock that was
// required for the OnEvictedFromWiredList method that the wired list worker routine
// was calling.
func TestSeriesWiredListConcurrentInteractions(t *testing.T) {
	var (
		runtimeOptsMgr = runtime.NewOptionsManager()
		runtimeOpts    = runtime.NewOptions().SetMaxWiredBlocks(1)
	)
	runtimeOptsMgr.Update(runtimeOpts)

	runtime.NewOptions().SetMaxWiredBlocks(1)
	wl := block.NewWiredList(block.WiredListOptions{
		RuntimeOptionsManager: runtimeOptsMgr,
		InstrumentOptions:     instrument.NewOptions(),
		ClockOptions:          clock.NewOptions(),
		// Use a small channel to stress-test the implementation
		EventsChannelSize: 1,
	})
	wl.Start()
	defer wl.Stop()

	var (
		blOpts = testDatabaseOptions().DatabaseBlockOptions()
		blPool = block.NewDatabaseBlockPool(
			// Small pool size to make any pooling issues more
			// likely to manifest.
			pool.NewObjectPoolOptions().SetSize(5),
		)
	)
	blPool.Init(func() block.DatabaseBlock {
		return block.NewDatabaseBlock(time.Time{}, 0, ts.Segment{}, blOpts)
	})

	var (
		opts = testDatabaseOptions().SetDatabaseBlockOptions(
			blOpts.
				SetWiredList(wl).
				SetDatabaseBlockPool(blPool),
		)
		shard  = testDatabaseShard(t, opts)
		id     = ident.StringID("foo")
		series = series.NewDatabaseSeries(id, ident.Tags{}, shard.seriesOpts)
	)

	series.Reset(id, ident.Tags{}, nil, shard.seriesOnRetrieveBlock, shard, shard.seriesOpts)
	series.Bootstrap(nil)
	shard.Lock()
	shard.insertNewShardEntryWithLock(lookup.NewEntry(series, 0))
	shard.Unlock()

	var (
		wg        = sync.WaitGroup{}
		doneCh    = make(chan struct{})
		blockSize = 2 * time.Hour
	)
	go func() {
		// Try and trigger any pooling issues
		for {
			select {
			case <-doneCh:
				return
			default:
				bl := blPool.Get()
				bl.ResetRetrievable(time.Time{}, blockSize, nil, block.RetrievableBlockMetadata{})
				bl.Close()
			}
		}
	}()

	start := time.Now().Truncate(blockSize)
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			blTime := start
			start = start.Add(blockSize)
			shard.OnRetrieveBlock(id, nil, blTime, ts.Segment{})
			// shard.OnEvictedFromWiredList(id, blTime)
			wg.Done()
		}()
	}

	wg.Wait()
	close(doneCh)
}

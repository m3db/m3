package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/time"
)

func commitLogWriteNoOp(
	series commitlog.Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
) error {
	return nil
}

type dbNamespace struct {
	sync.RWMutex

	name     string
	shardSet sharding.ShardSet
	nopts    namespace.Options
	dopts    Options
	nowFn    clock.NowFn
	bs       bootstrapState

	// Contains an entry to all shards for fast shard lookup, an
	// entry will be nil when this shard does not belong to current database
	shards []databaseShard

	increasingIndex  increasingIndex
	writeCommitLogFn writeCommitLogFn
}

func newDatabaseNamespace(
	metadata namespace.Metadata,
	shardSet sharding.ShardSet,
	increasingIndex increasingIndex,
	writeCommitLogFn writeCommitLogFn,
	dopts Options,
) databaseNamespace {
	name := metadata.Name()
	nopts := metadata.Options()
	fn := writeCommitLogFn
	if !nopts.GetWritesToCommitLog() {
		fn = commitLogWriteNoOp
	}

	n := &dbNamespace{
		name:             name,
		shardSet:         shardSet,
		nopts:            nopts,
		dopts:            dopts,
		nowFn:            dopts.GetClockOptions().GetNowFn(),
		increasingIndex:  increasingIndex,
		writeCommitLogFn: fn,
	}

	n.initShards()

	return n
}

func (n *dbNamespace) Name() string {
	return n.name
}

func (n *dbNamespace) Tick() {
	shards := n.getOwnedShards()
	if len(shards) == 0 {
		return
	}

	// Tick through the shards sequentially to avoid parallel data flushes
	for _, shard := range shards {
		shard.Tick()
	}
}

func (n *dbNamespace) Write(
	ctx context.Context,
	id string,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	shardID := n.shardSet.Shard(id)
	shard, err := n.shardAt(shardID)
	if err != nil {
		return err
	}
	return shard.Write(ctx, id, timestamp, value, unit, annotation)
}

func (n *dbNamespace) ReadEncoded(
	ctx context.Context,
	id string,
	start, end time.Time,
) ([][]xio.SegmentReader, error) {
	shardID := n.shardSet.Shard(id)
	shard, err := n.shardAt(shardID)
	if err != nil {
		return nil, err
	}
	return shard.ReadEncoded(ctx, id, start, end)
}

func (n *dbNamespace) FetchBlocks(
	ctx context.Context,
	shardID uint32,
	id string,
	starts []time.Time,
) ([]FetchBlockResult, error) {
	shard, err := n.shardAt(shardID)
	if err != nil {
		return nil, xerrors.NewInvalidParamsError(err)
	}
	return shard.FetchBlocks(ctx, id, starts), nil
}

func (n *dbNamespace) FetchBlocksMetadata(
	ctx context.Context,
	shardID uint32,
	limit int64,
	pageToken int64,
	includeSizes bool,
) ([]FetchBlocksMetadataResult, *int64, error) {
	shard, err := n.shardAt(shardID)
	if err != nil {
		return nil, nil, xerrors.NewInvalidParamsError(err)
	}
	res, nextPageToken := shard.FetchBlocksMetadata(ctx, limit, pageToken, includeSizes)
	return res, nextPageToken, nil
}

func (n *dbNamespace) Bootstrap(
	bs bootstrap.Bootstrap,
	writeStart time.Time,
	cutover time.Time,
) error {
	n.Lock()
	if n.bs == bootstrapped {
		n.Unlock()
		return nil
	}
	if n.bs == bootstrapping {
		n.Unlock()
		return errNamespaceIsBootstrapping
	}
	n.bs = bootstrapping
	n.Unlock()

	if !n.nopts.GetNeedsBootstrap() {
		n.Lock()
		n.bs = bootstrapped
		n.Unlock()
		return nil
	}

	multiErr := xerrors.NewMultiError()
	shards := n.getOwnedShards()
	for _, shard := range shards {
		err := shard.Bootstrap(bs, n.name, writeStart, cutover)
		multiErr = multiErr.Add(err)
	}

	n.Lock()
	n.bs = bootstrapped
	n.Unlock()

	return multiErr.FinalError()
}

func (n *dbNamespace) Flush(
	ctx context.Context,
	blockStart time.Time,
	pm persist.Manager,
) error {
	n.RLock()
	if n.bs != bootstrapped {
		n.RUnlock()
		return errNamespaceNotBootstrapped
	}
	n.RUnlock()

	if !n.nopts.GetNeedsFlush() {
		return nil
	}

	multiErr := xerrors.NewMultiError()
	shards := n.getOwnedShards()
	for _, shard := range shards {
		// NB(xichen): we still want to proceed if a shard fails to flush its data.
		// Probably want to emit a counter here, but for now just log it.
		if err := shard.Flush(ctx, n.name, blockStart, pm); err != nil {
			detailedErr := fmt.Errorf("shard %d failed to flush data: %v", shard.ID(), err)
			multiErr = multiErr.Add(detailedErr)
		}
	}

	return multiErr.FinalError()
}

func (n *dbNamespace) CleanupFileset(earliestToRetain time.Time) error {
	if !n.nopts.GetNeedsFilesetCleanup() {
		return nil
	}

	multiErr := xerrors.NewMultiError()
	shards := n.getOwnedShards()
	for _, shard := range shards {
		if err := shard.CleanupFileset(n.name, earliestToRetain); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	return multiErr.FinalError()
}

func (n *dbNamespace) Truncate() {
	n.initShards()

	// NB(xichen): possibly also clean up disk files and force a GC here to reclaim memory immediately
}

func (n *dbNamespace) getOwnedShards() []databaseShard {
	n.RLock()
	shards := n.shardSet.Shards()
	databaseShards := make([]databaseShard, len(shards))
	for i, shard := range shards {
		databaseShards[i] = n.shards[shard]
	}
	n.RUnlock()
	return databaseShards
}

func (n *dbNamespace) shardAt(shardID uint32) (databaseShard, error) {
	n.RLock()
	shard := n.shards[shardID]
	n.RUnlock()
	if shard == nil {
		return nil, fmt.Errorf("not responsible for shard %d", shardID)
	}
	return shard, nil
}

func (n *dbNamespace) initShards() {
	shards := n.shardSet.Shards()
	dbShards := make([]databaseShard, n.shardSet.Max()+1)
	for _, shard := range shards {
		dbShards[shard] = newDatabaseShard(shard, n.increasingIndex, n.writeCommitLogFn, n.dopts)
	}
	n.Lock()
	n.shards = dbShards
	n.Unlock()
}

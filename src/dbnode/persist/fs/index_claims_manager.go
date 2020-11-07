package fs

import (
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

type indexClaimsManager struct {
	sync.Mutex

	filePathPrefix                string
	nowFn                         clock.NowFn
	nextIndexFileSetVolumeIndexFn nextIndexFileSetVolumeIndexFn

	// Map of ns ID string -> blockStart -> volumeIndexClaim.
	volumeIndexClaims map[string]map[xtime.UnixNano]volumeIndexClaim
}

type volumeIndexClaim struct {
	volumeIndex int
	blockStart  xtime.UnixNano
}

// NewIndexClaimsManager returns an instance of the index claim manager. This manages
// concurrent claims for volume indices per ns and block start.
// NB(bodu): There should be only a single shared index claim manager among all threads
// writing index data filesets.
func NewIndexClaimsManager(opts Options) IndexClaimsManager {
	return &indexClaimsManager{
		filePathPrefix:                opts.FilePathPrefix(),
		nowFn:                         opts.ClockOptions().NowFn(),
		volumeIndexClaims:             make(map[string]map[xtime.UnixNano]volumeIndexClaim),
		nextIndexFileSetVolumeIndexFn: NextIndexFileSetVolumeIndex,
	}
}

func (i *indexClaimsManager) ClaimNextIndexFileSetVolumeIndex(
	md namespace.Metadata,
	blockStart time.Time,
) (int, error) {
	i.Lock()
	defer func() {
		i.deleteOutOfRetentionEntriesWithLock(md.ID(), md.Options())
		i.Unlock()
	}()
	volumeIndexClaimsByBlockStart, ok := i.volumeIndexClaims[md.ID().String()]
	if !ok {
		volumeIndexClaimsByBlockStart = make(map[xtime.UnixNano]volumeIndexClaim)
		i.volumeIndexClaims[md.ID().String()] = volumeIndexClaimsByBlockStart
	}

	blockStartUnixNanos := xtime.ToUnixNano(blockStart)
	if curr, ok := volumeIndexClaimsByBlockStart[blockStartUnixNanos]; ok {
		// Already had a previous claim, return the next claim.
		next := curr
		next.volumeIndex++
		volumeIndexClaimsByBlockStart[blockStartUnixNanos] = next
		return next.volumeIndex, nil
	}

	volumeIndex, err := i.nextIndexFileSetVolumeIndexFn(i.filePathPrefix, md.ID(),
		blockStart)
	if err != nil {
		return 0, err
	}
	volumeIndexClaimsByBlockStart[blockStartUnixNanos] = volumeIndexClaim{
		volumeIndex: volumeIndex,
		blockStart:  blockStartUnixNanos,
	}
	return volumeIndex, nil
}

func (i *indexClaimsManager) deleteOutOfRetentionEntriesWithLock(
	nsID ident.ID,
	opts namespace.Options,
) {
	earliestBlockStart := retention.FlushTimeStartForRetentionPeriod(
		opts.RetentionOptions().RetentionPeriod(),
		opts.IndexOptions().BlockSize(),
		i.nowFn(),
	)
	earliestBlockStartUnixNanos := xtime.ToUnixNano(earliestBlockStart)
	// ns ID already exists at this point since the delete call is deferred.
	for blockStart := range i.volumeIndexClaims[nsID.String()] {
		if blockStart.Before(earliestBlockStartUnixNanos) {
			delete(i.volumeIndexClaims[nsID.String()], blockStart)
		}
	}
}

type nextIndexFileSetVolumeIndexFn func(
	filePathPrefix string,
	namespace ident.ID,
	blockStart time.Time,
) (int, error)

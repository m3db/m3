package persist

import (
	"fmt"
	"time"

	"github.com/m3db/m3db/storage/namespace"
)

// PrepareOptionsMatcher satisfies the gomock.Matcher interface for PrepareOptions
type PrepareOptionsMatcher struct {
	NsMetadata namespace.Metadata
	Shard      uint32
	BlockStart time.Time
	WrittenAt  time.Time
}

// Matches determines whether a PrepareOptionsMatcher matches a PrepareOptions
func (p PrepareOptionsMatcher) Matches(x interface{}) bool {
	prepareOptions, ok := x.(PrepareOptions)
	if !ok {
		return false
	}

	if !p.NsMetadata.Equal(prepareOptions.NsMetadata) {
		return false
	}
	if p.Shard != prepareOptions.Shard {
		return false
	}
	if !p.WrittenAt.Equal(prepareOptions.WrittenAt) {
		return false
	}
	if !p.BlockStart.Equal(prepareOptions.BlockStart) {
		return false
	}

	return true
}

func (p PrepareOptionsMatcher) String() string {
	return fmt.Sprintf(
		"id: %s, shard: %d, BlockStart: %d",
		p.NsMetadata.ID().String(), p.Shard, p.WrittenAt.Unix())
}

package persist

import (
	"fmt"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3db/storage/namespace"
)

type PrepareOptionsMatcherIFace interface {
	gomock.Matcher
}

// PrepareOptionsMatcher satisfies the gomock.Matcher interface for PrepareOptions
type PrepareOptionsMatcher struct {
	NsMetadata namespace.Metadata
	Shard      uint32
	BlockStart time.Time
}

func (p PrepareOptionsMatcher) ToIFace() PrepareOptionsMatcherIFace {
	return &p
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
	if !p.BlockStart.Equal(prepareOptions.PersistTime) {
		return false
	}

	return true
}

func (p PrepareOptionsMatcher) String() string {
	return fmt.Sprintf(
		"id: %s, shard: %d, BlockStart: %d",
		p.NsMetadata.ID().String(), p.Shard, p.BlockStart.Unix())
}

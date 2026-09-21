package fs

import (
	"testing"
	"time"

	protobuftypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/dbnode/generated/proto/index"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestNewSegments(t *testing.T) {
	blockStart := time.Now().Truncate(time.Hour)
	blockSize := time.Hour * 2
	volumeIndex := 3
	volumeTypeValue := "default"
	absolutePaths := []string{"/var/lib/m3db/index1", "/var/lib/m3db/index2"}
	shards := []uint32{1, 2}

	info := index.IndexVolumeInfo{
		BlockStart: blockStart.UnixNano(),
		BlockSize:  int64(blockSize),
		Shards:     shards,
		IndexVolumeType: &protobuftypes.StringValue{
			Value: volumeTypeValue,
		},
	}

	segments := NewSegments(info, volumeIndex, absolutePaths)

	assert.NotNil(t, segments)
	assert.Equal(t, xtime.UnixNano(blockStart.UnixNano()), segments.BlockStart())
	assert.Equal(t, volumeIndex, segments.VolumeIndex())
	assert.Equal(t, idxpersist.IndexVolumeType(volumeTypeValue), segments.VolumeType())
	assert.Equal(t, absolutePaths, segments.AbsoluteFilePaths())

	str := segments.ShardTimeRanges()
	assert.Len(t, str, len(shards))

	expectedRanges := xtime.NewRanges(xtime.Range{
		Start: xtime.UnixNano(blockStart.UnixNano()),
		End:   xtime.UnixNano(blockStart.UnixNano()).Add(blockSize),
	})

	for _, shard := range shards {
		actualRanges, ok := str.Get(shard)
		assert.True(t, ok)
		assert.Equal(t, expectedRanges.String(), actualRanges.String())
	}
}

package m3

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var benchmarkIsRollupIDResult bool

func BenchmarkIsRollupID(b *testing.B) {
	id := []byte("m3+foo+m3_rollup=true,tagName1=tagValue1")

	name, tags, err := NameAndTags(id)
	require.NoError(b, err)

	assert.Equal(b, []byte("foo"), name)
	assert.Equal(b, []byte("m3_rollup=true,tagName1=tagValue1"), tags)
	assert.True(b, IsRollupID(name, tags))

	b.ReportAllocs()
	b.ResetTimer()

	loopResult := false
	for i := 0; i < b.N; i++ {
		result := false
		for j := 0; j < 100_000; j++ {
			result = IsRollupID(name, tags)
		}
		loopResult = result
	}

	benchmarkIsRollupIDResult = loopResult
}

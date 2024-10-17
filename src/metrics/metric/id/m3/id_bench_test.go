package m3

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Results 2024-10-17:
// goos: darwin
// goarch: arm64
// pkg: github.com/m3db/m3/src/metrics/metric/id/m3
// BenchmarkIsRollupID
// BenchmarkIsRollupID-10    	     349	   3404269 ns/op	       0 B/op	       0 allocs/op
// PASS

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

	require.True(b, loopResult)
}

// Test_IsRollupID_doesntAlloc is a sanity check that IsRollupID doesn't require any iterator allocations.
// If you hit a failure on this assertion, it *could* be okay to change it to allow for some allocations.
// However, this is a fairly performance sensitive path--think carefully about what you're doing.
// It's also possible that a Go version upgrade will change this; consider whether it makes sense to tweak
// the approach at that time.
func Test_IsRollupID_doesntAlloc(t *testing.T) {
	id := []byte("m3+foo+m3_rollup=true,tagName1=tagValue1")

	name, tags, err := NameAndTags(id)
	require.NoError(t, err)

	allocated := testing.AllocsPerRun(10, func() {
		IsRollupID(name, tags)
	})

	assert.Equal(t, 0.0, allocated, "IsRollupID should not require allocating memory")
}

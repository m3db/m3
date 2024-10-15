package tagfiltertree

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPointerSetCountBits(t *testing.T) {
	tests := []struct {
		name     string
		setBits  []uint64
		expected int
	}{
		{
			name:     "empty set",
			setBits:  []uint64{0, 0},
			expected: 0,
		},
		{
			name:     "single set bit",
			setBits:  []uint64{0, 1},
			expected: 1,
		},
		{
			name:     "multiple set bits",
			setBits:  []uint64{7, 7},
			expected: 6,
		},
		{
			name:     "all set bits",
			setBits:  []uint64{math.MaxUint64, math.MaxUint64},
			expected: 128,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps := PointerSet{}
			l := tt.setBits[0]
			r := tt.setBits[1]
			var i byte
			for i = 0; i < 128; i++ {
				if i < 64 {
					if l&0x1 == 1 {
						ps.Set(i)
					}
					l >>= 1
				} else {
					if r&0x1 == 1 {
						ps.Set(i)
					}
					r >>= 1
				}
			}

			require.Equal(t, tt.expected, ps.CountSetBitsUntil(127))
		})
	}
}

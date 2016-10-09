package ts

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConstructorEquality(t *testing.T) {
	a := StringID("abc")
	b := BinaryID([]byte{'a', 'b', 'c'})

	assert.Equal(t, a.Data(), b.Data())
	assert.Equal(t, a.Hash(), b.Hash())
	assert.Equal(t, a.String(), b.String())

	assert.True(t, a.Equal(b))
}

func BenchmarkHashing(b *testing.B) {
	for i := 0; i < b.N; i++ {
		id := BinaryID([]byte{byte(i)})
		id.Hash()
	}
}

func BenchmarkHashCaching(b *testing.B) {
	for i := 0; i < b.N; i++ {
		id := BinaryID([]byte{byte(i)})
		id.Hash()
		id.Hash()
	}
}

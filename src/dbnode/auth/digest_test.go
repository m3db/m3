package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateHash(t *testing.T) {
	input := "teststring"
	expectedOutput := "d67c5cbf5b01c9f91932e3b8def5e5f8"

	// output after cache miss.
	output, err := GenerateHash(input)
	assert.NoError(t, err)
	assert.Equal(t, expectedOutput, output)

	// output from cache.
	cacheoutput, cacheerr := GenerateHash(input)
	assert.NoError(t, cacheerr)
	assert.Equal(t, expectedOutput, cacheoutput)
}

func TestGenerateHashInvalidatingCache(t *testing.T) {
	input := "teststring"
	expectedOutput := "d67c5cbf5b01c9f91932e3b8def5e5f8"

	// output after cache miss.
	output, err := GenerateHash(input)
	assert.NoError(t, err)
	assert.Equal(t, expectedOutput, output)

	invalidateCache()

	// output after cache miss.
	nonCachedout, nonCachederr := GenerateHash(input)
	assert.NoError(t, nonCachederr)
	assert.Equal(t, expectedOutput, nonCachedout)

	// output from cache.
	cacheoutput, cacheerr := GenerateHash(input)
	assert.NoError(t, cacheerr)
	assert.Equal(t, expectedOutput, cacheoutput)
}

package auth

import (
	mrand "math/rand"
	"testing"
)

const (
	charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

var (
	inputStr = 10
)

func BenchmarkGetMD5DigestMap(b *testing.B) {
	testInput := []string{}
	for i := 0; i < inputStr; i++ {
		testInput = append(testInput, RandomString(10))
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := GenerateHash(testInput[i%inputStr]); err != nil {
			b.Fatalf("error generating MD5 digest: %v", err)
		}
	}
}

func RandomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[mrand.Intn(len(charset))]
	}
	return string(b)
}

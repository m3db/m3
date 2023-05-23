package auth

import (
	"crypto/md5" // #nosec
	"encoding/hex"
	"fmt"
	"hash"
)

var (
	credentialCache = map[string]string{}
	hashFunc        = md5.New() // #nosec
)

func generateHashWithCacheLookup(data string, h hash.Hash) (string, error) {
	if val, ok := credentialCache[data]; ok {
		return val, nil
	}
	_, err := h.Write([]byte(data))
	if err != nil {
		return "", fmt.Errorf("error generating hash")
	}
	generatedHash := hex.EncodeToString(h.Sum(nil))
	credentialCache[data] = generatedHash
	return generatedHash, nil
}

// GenerateHash takes data as input param and returns md5 hash either from cache or creating md5 hash on runtime.
func GenerateHash(data string) (string, error) {
	defer hashFunc.Reset()
	return generateHashWithCacheLookup(data, hashFunc)
}

func invalidateCache() {
	credentialCache = map[string]string{}
}

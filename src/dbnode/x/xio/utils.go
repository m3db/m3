package xio

import "encoding/binary"

// ToBytes reads and returns the contents of Reader64 as a slice of bytes.
// Should normally return io.EOF as an error.
func ToBytes(reader Reader64) ([]byte, error) {
	var (
		res []byte
		buf [8]byte
	)

	word, bytes, err := reader.Read64()
	for ; err == nil; word, bytes, err = reader.Read64() {
		binary.BigEndian.PutUint64(buf[:], word)
		res = append(res, buf[:bytes]...)
	}

	return res, err
}

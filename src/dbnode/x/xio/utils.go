package xio

import "encoding/binary"

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

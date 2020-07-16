package msgpack

import (
	"errors"
	"hash"
	"hash/adler32"
)

var (
	// errChecksumMismatch returned when the calculated checksum doesn't match the stored checksum
	errChecksumMismatch = errors.New("calculated checksum doesn't match stored checksum")
)

// DecoderStreamWithDigest calculates the digest as it processes a decoder stream.
type DecoderStreamWithDigest interface {
	DecoderStream

	// Reset resets the reader for use with a new reader.
	Reset(stream DecoderStream)

	// Digest returns the digest
	Digest() hash.Hash32

	// Validate compares the current digest against the expected digest and returns
	// an error if they don't match.
	Validate(expectedDigest uint32) error

	// Capture provides a mechanism for manually capturing bytes to add to digest when reader is manipulated
	// through atypical means (e.g. reading directly from the backing byte slice of a ByteReader)
	Capture(bytes []byte) error

	// Returns the decoder stream wrapped by this object
	wrappedStream() DecoderStream
}

type decoderStreamWithDigest struct {
	reader     DecoderStream
	digest     hash.Hash32
	unreadByte bool
}

func newDecoderStreamWithDigest(reader DecoderStream) DecoderStreamWithDigest {
	return &decoderStreamWithDigest{
		reader: reader,
		digest: adler32.New(),
	}
}

func (d *decoderStreamWithDigest) Read(p []byte) (n int, err error) {
	n, err = d.reader.Read(p)
	if n > 0 {
		start := 0
		if d.unreadByte {
			d.unreadByte = false
			start++
		}
		if _, err := d.digest.Write(p[start:n]); err != nil {
			return 0, err
		}
	}
	return n, err
}

func (d *decoderStreamWithDigest) ReadByte() (byte, error) {
	b, err := d.reader.ReadByte()
	if err == nil {
		if d.unreadByte {
			d.unreadByte = false
		} else {
			if _, err := d.digest.Write([]byte{b}); err != nil {
				return b, err
			}
		}
	}
	return b, err
}

func (d *decoderStreamWithDigest) UnreadByte() error {
	err := d.reader.UnreadByte()
	if err == nil {
		d.unreadByte = true
	}
	return err
}

func (d *decoderStreamWithDigest) Reset(stream DecoderStream) {
	d.reader = stream
	d.digest.Reset()
}

func (d *decoderStreamWithDigest) Digest() hash.Hash32 {
	return d.digest
}

func (d *decoderStreamWithDigest) Validate(expectedDigest uint32) error {
	if d.digest.Sum32() != expectedDigest {
		return errChecksumMismatch
	}
	return nil
}

func (d *decoderStreamWithDigest) Capture(bytes []byte) error {
	// No-op if not actually capturing at the moment
	if d.reader != nil {
		if _, err := d.digest.Write(bytes); err != nil {
			return err
		}
	}
	return nil
}

func (d *decoderStreamWithDigest) wrappedStream() DecoderStream {
	return d.reader
}

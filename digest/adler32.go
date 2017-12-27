// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package digest

// TODO: attribute properly rather than just pulling source file in.
// Adler is defined in RFC 1950:
//	Adler-32 is composed of two sums accumulated per byte: s1 is
//	the sum of all bytes, s2 is the sum of all s1 values. Both sums
//	are done modulo 65521. s1 is initialized to 1, s2 to zero.  The
//	Adler-32 checksum is stored as s2*65536 + s1 in most-
//	significant-byte first (network) order.

import "hash"

const (
	// mod is the largest prime that is less than 65536.
	mod = 65521
	// nmax is the largest n such that
	// 255 * n * (n+1) / 2 + (n+1) * (mod-1) <= 2^32-1.
	// It is mentioned in RFC 1950 (search for "5552").
	nmax = 5552

	resetValue = uint32(1)
)

const digestAdler32Size = 4

// digest represents the partial evaluation of a checksum.
// The low 16 bits are s1, the high 16 bits are s2.
type digestAdler32 uint32

// hash32 is a hash.Hash32 interface that allows resetting to a
// specific value.
type hash32 interface {
	hash.Hash32
	ResetTo(value uint32)
}

// New returns a new hash.Hash32 computing the Adler-32 checksum.
// Its Sum method will lay the value out in big-endian byte order.
func newAdler32() hash32 {
	d := newResetAdler32()
	return &d
}

func newResetAdler32() digestAdler32 {
	return digestAdler32(resetValue)
}

func (d *digestAdler32) Reset()           { d.ResetTo(resetValue) }
func (d *digestAdler32) ResetTo(v uint32) { *d = digestAdler32(v) }

func (d *digestAdler32) Size() int { return digestAdler32Size }

func (d *digestAdler32) BlockSize() int { return 4 }

// Add p to the running checksum d.
func update(d digestAdler32, p []byte) digestAdler32 {
	s1, s2 := uint32(d&0xffff), uint32(d>>16)
	for len(p) > 0 {
		var q []byte
		if len(p) > nmax {
			p, q = p[:nmax], p[nmax:]
		}
		for len(p) >= 4 {
			s1 += uint32(p[0])
			s2 += s1
			s1 += uint32(p[1])
			s2 += s1
			s1 += uint32(p[2])
			s2 += s1
			s1 += uint32(p[3])
			s2 += s1
			p = p[4:]
		}
		for _, x := range p {
			s1 += uint32(x)
			s2 += s1
		}
		s1 %= mod
		s2 %= mod
		p = q
	}
	return digestAdler32(s2<<16 | s1)
}

func (d *digestAdler32) Write(p []byte) (nn int, err error) {
	*d = update(*d, p)
	return len(p), nil
}

func (d *digestAdler32) Sum32() uint32 { return uint32(*d) }

func (d *digestAdler32) Sum(in []byte) []byte {
	s := uint32(*d)
	return append(in, byte(s>>24), byte(s>>16), byte(s>>8), byte(s))
}

// Checksum returns the Adler-32 checksum of data.
func Checksum(data []byte) uint32 { return uint32(update(1, data)) }

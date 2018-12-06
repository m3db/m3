// Copyright (c) 2018 Uber Technologies, Inc
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE

package msgpack

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkLogEntryDecoder(b *testing.B) {
	var (
		enc    = NewEncoder()
		dec    = NewDecoder(nil)
		stream = NewDecoderStream(nil)
		err    error
	)

	require.NoError(b, enc.EncodeLogEntry(testLogEntry))
	buf := enc.Bytes()
	for n := 0; n < b.N; n++ {
		stream.Reset(buf)
		dec.Reset(stream)
		_, err = dec.DecodeLogEntry()
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkLogEntryDecoderFast(b *testing.B) {
	var (
		enc = NewEncoder()
		err error
	)

	require.NoError(b, enc.EncodeLogEntry(testLogEntry))
	buf := enc.Bytes()
	for n := 0; n < b.N; n++ {
		_, err = DecodeLogEntryFast(buf)
		if err != nil {
			panic(err)
		}
	}
}

var benchmarkBuf []byte

func BenchmarkLogEntryEncoderFast(b *testing.B) {
	var err error
	benchmarkBuf = []byte{}

	for n := 0; n < b.N; n++ {
		benchmarkBuf, err = EncodeLogEntryFast(benchmarkBuf[:0], testLogEntry)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkLogEntryEncoder(b *testing.B) {
	var (
		enc = NewEncoder()
		err error
	)

	for n := 0; n < b.N; n++ {
		enc.EncodeLogEntry(testLogEntry)
		if err != nil {
			panic(err)
		}
		benchmarkBuf = enc.Bytes()
	}
}

func BenchmarkLogMetadataEncoder(b *testing.B) {
	var (
		enc = NewEncoder()
		err error
	)

	for n := 0; n < b.N; n++ {
		enc.EncodeLogMetadata(testLogMetadata)
		if err != nil {
			panic(err)
		}
		benchmarkBuf = enc.Bytes()
	}
}

func BenchmarkLogMetadataEncoderFast(b *testing.B) {
	var err error
	benchmarkBuf = []byte{}

	for n := 0; n < b.N; n++ {
		benchmarkBuf, err = EncodeLogMetadataFast(benchmarkBuf[:0], testLogMetadata)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkLogMetadataDecoder(b *testing.B) {
	var (
		enc    = NewEncoder()
		dec    = NewDecoder(nil)
		stream = NewDecoderStream(nil)
		err    error
	)

	require.NoError(b, enc.EncodeLogMetadata(testLogMetadata))
	buf := enc.Bytes()
	for n := 0; n < b.N; n++ {
		stream.Reset(buf)
		dec.Reset(stream)
		_, err = dec.DecodeLogMetadata()
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkLogMetadataDecoderFast(b *testing.B) {
	var (
		enc = NewEncoder()
		err error
	)

	require.NoError(b, enc.EncodeLogMetadata(testLogMetadata))
	buf := enc.Bytes()
	for n := 0; n < b.N; n++ {
		_, err = DecodeLogMetadataFast(buf)
		if err != nil {
			panic(err)
		}
	}
}

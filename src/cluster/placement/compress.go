// Copyright (c) 2021 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package placement

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/klauspost/compress/zstd"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
)

const _decoderInitialBufferSize = 131072

var (
	_zstdEncoder *zstd.Encoder
	_zstdDecoder *zstd.Decoder
)

func compressPlacementProto(p *placementpb.Placement) ([]byte, error) {
	if p == nil {
		return nil, errNilPlacementSnapshotsProto
	}

	uncompressed, _ := p.Marshal()

	compressed := make([]byte, 0, len(uncompressed)/2) // prealloc, assuming at least 50% space savings
	compressed = _zstdEncoder.EncodeAll(uncompressed, compressed)

	return compressed, nil
}

func decompressPlacementProto(compressed []byte) (*placementpb.Placement, error) {
	if compressed == nil {
		return nil, errNilValue
	}

	var (
		decompressed = make([]byte, 0, _decoderInitialBufferSize)
		err          error
	)

	decompressed, err = _zstdDecoder.DecodeAll(compressed, decompressed)
	if err != nil {
		return nil, err
	}

	result := &placementpb.Placement{}
	if err := proto.Unmarshal(decompressed, result); err != nil {
		return nil, err
	}

	return result, nil
}

func init() {
	ropts := []zstd.DOption{
		zstd.WithDecoderLowmem(false),
	}

	r, err := zstd.NewReader(nil, ropts...)
	if err != nil {
		panic(fmt.Errorf("error initializing zstd reader: %w", err))
	}
	_zstdDecoder = r

	opts := []zstd.EOption{
		zstd.WithEncoderCRC(true),
		zstd.WithEncoderLevel(zstd.SpeedBestCompression),
	}
	w, err := zstd.NewWriter(nil, opts...)
	if err != nil {
		panic(fmt.Errorf("error initializing zstd writer: %w", err))
	}
	_zstdEncoder = w
}

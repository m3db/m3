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
	"github.com/gogo/protobuf/proto"
	"github.com/klauspost/compress/zstd"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
)

const _decoderInitialBufferSize = 131072

func compressPlacementProto(p *placementpb.Placement) ([]byte, error) {
	if p == nil {
		return nil, errNilPlacementSnapshotsProto
	}

	w, err := zstd.NewWriter(
		nil,
		zstd.WithEncoderCRC(true),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		return nil, err
	}
	defer w.Close()

	uncompressed, _ := p.Marshal()

	compressed := make([]byte, 0, len(uncompressed)/2) // prealloc, assuming at least 50% space savings
	compressed = w.EncodeAll(uncompressed, compressed)

	return compressed, nil
}

func decompressPlacementProto(compressed []byte) (*placementpb.Placement, error) {
	if compressed == nil {
		return nil, errNilValue
	}

	decompressed := make([]byte, 0, _decoderInitialBufferSize)
	r, err := zstd.NewReader(
		nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(false),
	)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	decompressed, err = r.DecodeAll(compressed, decompressed)
	if err != nil {
		return nil, err
	}

	result := &placementpb.Placement{}
	if err := proto.Unmarshal(decompressed, result); err != nil {
		return nil, err
	}

	return result, nil
}

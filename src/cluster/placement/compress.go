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
	"bytes"
	"compress/flate"
	"io/ioutil"

	"github.com/gogo/protobuf/proto"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
)

func compressPlacementProto(proto *placementpb.Placement) ([]byte, error) {
	uncompressed, err := proto.Marshal()
	if err != nil {
		return nil, err
	}

	var compressed bytes.Buffer
	w, err := flate.NewWriter(&compressed, flate.BestCompression)
	if err != nil {
		return nil, err
	}

	if _, err := w.Write(uncompressed); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return compressed.Bytes(), nil
}

func decompressPlacementProto(compressed []byte) (*placementpb.Placement, error) {
	r := flate.NewReader(bytes.NewReader(compressed))
	decompressed, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	if err := r.Close(); err != nil {
		return nil, err
	}

	result := &placementpb.Placement{}
	if err := proto.Unmarshal(decompressed, result); err != nil {
		return nil, err
	}

	return result, nil
}

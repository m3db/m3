// Copyright (c) 2017 Uber Technologies, Inc.
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

package fs

import (
	"github.com/m3db/m3db/src/dbnode/digest"
)

// filesetDigests is a container struct for storing a digest for all of the
// fileset files for a given shard / block combination
type filesetDigests struct {
	infoDigest        uint32
	indexDigest       uint32
	summariesDigest   uint32
	bloomFilterDigest uint32
	dataDigest        uint32
}

// note that caller is responsible for calling Validate()
func readFileSetDigests(
	digestFdWithDigestContents digest.FdWithDigestContentsReader,
) (filesetDigests, error) {
	var (
		fsDigests filesetDigests
		err       error
	)

	if fsDigests.infoDigest, err = digestFdWithDigestContents.ReadDigest(); err != nil {
		return fsDigests, err
	}
	if fsDigests.indexDigest, err = digestFdWithDigestContents.ReadDigest(); err != nil {
		return fsDigests, err
	}
	if fsDigests.summariesDigest, err = digestFdWithDigestContents.ReadDigest(); err != nil {
		return fsDigests, err
	}
	if fsDigests.bloomFilterDigest, err = digestFdWithDigestContents.ReadDigest(); err != nil {
		return fsDigests, err
	}
	if fsDigests.dataDigest, err = digestFdWithDigestContents.ReadDigest(); err != nil {
		return fsDigests, err
	}

	return fsDigests, nil
}

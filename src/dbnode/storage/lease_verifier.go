// Copyright (c) 2019 Uber Technologies, Inc.
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

package storage

import (
	"fmt"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/x/ident"
)

type flushStateRetriever interface {
	FlushState(namespace ident.ID, shardID uint32, blockStart time.Time) (fileOpState, error)
}

type leaseVerifier struct {
	flushStateRetriever flushStateRetriever
}

func newLeaseVerifier(retriever flushStateRetriever) *leaseVerifier {
	return &leaseVerifier{
		flushStateRetriever: retriever,
	}
}

func (v *leaseVerifier) VerifyLease(
	descriptor block.LeaseDescriptor,
	state block.LeaseState,
) error {
	flushState, err := v.flushStateRetriever.FlushState(
		descriptor.Namespace, uint32(descriptor.Shard), descriptor.BlockStart)
	if err != nil {
		return fmt.Errorf(
			"err retrieving flushState for lease verification, ns: %s, shard: %d, blockStart: %s, err: %v",
			descriptor.Namespace.String(), descriptor.Shard, descriptor.BlockStart.String(), err)
	}

	if flushState.ColdVersion != state.Volume {
		// The cold flush version and volume correspond 1:1 so a lease should only
		// be permitted if the requested volume is equal to the highest flushed
		// volume (the current cold flush version).
		//
		// This logic also holds in situations where the cold flush feature is not
		// enabled because even in that case the volume number for the first warm
		// flush should be 0 and the cold version should also be 0.
		return fmt.Errorf(
			"cannot permit lease for ns: %s, shard: %d, blockStart: %s, volume: %d when latest volume is %d",
			descriptor.Namespace.String(), descriptor.Shard, descriptor.BlockStart.String(), state.Volume, flushState.ColdVersion)
	}

	return nil
}

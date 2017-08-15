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

package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"

	"github.com/golang/mock/gomock"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/uber-go/tally"
)

const (
	commitLogTestRandomSeeed        int64 = 7823434
	commitLogTestMinSuccessfulTests       = 10000
)

func newPropTestCleanupMgr(ctrl *gomock.Controller, ropts retention.Options, ns ...databaseNamespace) *cleanupManager {
	db := NewMockdatabase(ctrl)
	opts := testDatabaseOptions()
	opts = opts.SetCommitLogOptions(
		opts.CommitLogOptions().SetRetentionOptions(ropts))
	db.EXPECT().Options().Return(opts).AnyTimes()
	db.EXPECT().GetOwnedNamespaces().Return(ns).AnyTimes()
	scope := tally.NoopScope
	cm := newCleanupManager(db, scope)
	return cm.(*cleanupManager)
}

func newCleanupMgrTestProperties() *gopter.Properties {
	parameters := gopter.DefaultTestParameters()
	parameters.Rng.Seed(commitLogTestRandomSeeed) // generate reproducible results
	parameters.MinSuccessfulTests = commitLogTestMinSuccessfulTests
	return gopter.NewProperties(parameters)
}

func TestPropertyCommitLogNotCleanedForUnflushedData(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	properties := newCleanupMgrTestProperties()
	now := time.Now()
	timeWindow := time.Hour * 24 * 15

	properties.Property("Commit log is retained if one namespace needs to flush", prop.ForAll(
		func(t time.Time, cRopts retention.Options, ns *generatedNamespace) (bool, error) {
			cm := newPropTestCleanupMgr(ctrl, cRopts, ns)
			_, cleanupTimes := cm.commitLogTimes(t)
			for _, ct := range cleanupTimes {
				s, e := commitLogNamespaceBlockTimes(ct, cRopts.BlockSize(), ns.ropts)
				if ns.NeedsFlush(s, e) {
					return false, fmt.Errorf("trying to cleanup commit log at %v, but ns needsFlush; (range: %v, %v)",
						ct.String(), s.String(), e.String())
				}
			}
			return true, nil
		},
		gen.TimeRange(now.Add(-timeWindow), 2*timeWindow).WithLabel("cleanup time"),
		genCommitLogRetention().WithLabel("commit log retention"),
		genNamespace(now).WithLabel("namespace"),
	))

	properties.TestingRun(t)
}

func TestPropertyCommitLogNotCleanedForUnflushedDataMultipleNs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	properties := newCleanupMgrTestProperties()
	now := time.Now()
	timeWindow := time.Hour * 24 * 15

	properties.Property("Commit log is retained if any namespace needs to flush", prop.ForAll(
		func(t time.Time, cRopts retention.Options, nses []*generatedNamespace) (bool, error) {
			dbNses := generatedNamespaces(nses).asDatabaseNamespace()
			cm := newPropTestCleanupMgr(ctrl, cRopts, dbNses...)
			_, cleanupTimes := cm.commitLogTimes(t)
			for _, ct := range cleanupTimes {
				for _, ns := range nses {
					s, e := commitLogNamespaceBlockTimes(ct, cRopts.BlockSize(), ns.Options().RetentionOptions())
					if ns.NeedsFlush(s, e) {
						return false, fmt.Errorf("trying to cleanup commit log at %v, but ns needsFlush; (range: %v, %v)",
							ct.String(), s.String(), e.String())
					}
				}
			}
			return true, nil
		},
		gen.TimeRange(now.Add(-timeWindow), 2*timeWindow).WithLabel("cleanup time"),
		genCommitLogRetention().WithLabel("commit log retention"),
		gen.SliceOfN(3, genNamespace(now)).WithLabel("namespaces"),
	))

	properties.TestingRun(t)
}

type generatedNamespaces []*generatedNamespace

func (n generatedNamespaces) asDatabaseNamespace() []databaseNamespace {
	nses := make([]databaseNamespace, 0, len(n))
	for _, ns := range n {
		nses = append(nses, ns)
	}
	return nses
}

// generated namespace struct
type generatedNamespace struct {
	databaseNamespace

	opts              namespace.Options
	ropts             *generatedRetention
	blockSize         time.Duration
	oldestBlock       time.Time
	newestBlock       time.Time
	needsFlushMarkers []bool
}

func (ns *generatedNamespace) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("\n\tretention: %v", ns.ropts.String()))

	buf.WriteString(fmt.Sprintf("\n\tneedsFlush: \n"))
	w := new(tabwriter.Writer)
	w.Init(&buf, 5, 0, 1, ' ', 0)
	fmt.Fprintln(w, "blockStart\tneedFlush\t")
	for i := range ns.needsFlushMarkers {
		t := ns.oldestBlock.Add(time.Duration(i) * ns.blockSize)
		fmt.Fprintf(w, "%v\t%v\t\n", t, ns.needsFlushMarkers[i])
	}
	w.Flush()

	return buf.String()
}

func (ns *generatedNamespace) blockIdx(t time.Time) int {
	idx := int(t.Truncate(ns.blockSize).Sub(ns.oldestBlock) / ns.blockSize)
	if idx < 0 {
		return 0
	}
	if idx >= len(ns.needsFlushMarkers) {
		return len(ns.needsFlushMarkers) - 1
	}
	return idx
}

func (ns *generatedNamespace) Options() namespace.Options {
	return ns.opts
}

func (ns *generatedNamespace) NeedsFlush(start, end time.Time) bool {
	if start.Before(ns.oldestBlock) && end.Before(ns.oldestBlock) {
		return false
	}
	if start.After(ns.newestBlock) && end.After(ns.newestBlock) {
		return false
	}
	sIdx, eIdx := ns.blockIdx(start), ns.blockIdx(end)
	for i := sIdx; i <= eIdx; i++ {
		if ns.needsFlushMarkers[i] {
			return true
		}
	}
	return false
}

// generator for generatedNamespace
func genNamespace(t time.Time) gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		rng := genParams.Rng
		ropts := newRandomRetention(rng)
		oldest := retention.FlushTimeStart(ropts, t)
		newest := retention.FlushTimeEnd(ropts, t)

		n := numIntervals(oldest, newest, ropts.BlockSize())
		flushStates := make([]bool, n)
		for i := range flushStates {
			flushStates[i] = rng.Float32() > 0.6 // flip a coin to get a bool
		}

		opts := namespace.NewOptions().SetRetentionOptions(ropts)

		ns := &generatedNamespace{
			opts:              opts,
			ropts:             ropts,
			blockSize:         ropts.BlockSize(),
			oldestBlock:       oldest,
			newestBlock:       newest,
			needsFlushMarkers: flushStates,
		}

		genResult := gopter.NewGenResult(ns, gopter.NoShrinker)
		genResult.Sieve = func(v interface{}) bool {
			ns := v.(*generatedNamespace)
			if len(ns.needsFlushMarkers) <= 0 {
				return false
			}
			return ns.ropts.Validate() == nil
		}
		return genResult
	}
}

func newRandomRetention(rng *rand.Rand) *generatedRetention {
	var (
		blockSizeMins    = maxInt(1, rng.Intn(60*12)) // 12 hours
		retentionMins    = maxInt(1, rng.Intn(40)) * blockSizeMins
		bufferPastMins   = maxInt(1, rng.Intn(blockSizeMins))
		bufferFutureMins = maxInt(1, rng.Intn(blockSizeMins))
	)

	return &generatedRetention{retention.NewOptions().
		SetRetentionPeriod(time.Duration(retentionMins) * time.Minute).
		SetBlockSize(time.Duration(blockSizeMins) * time.Minute).
		SetBufferPast(time.Duration(bufferPastMins) * time.Minute).
		SetBufferFuture(time.Duration(bufferFutureMins) * time.Minute)}
}

// generator for retention options
func genRetention() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		opts := newRandomRetention(genParams.Rng)
		genResult := gopter.NewGenResult(opts, gopter.NoShrinker)
		genResult.Sieve = func(v interface{}) bool {
			return v.(retention.Options).Validate() == nil
		}
		return genResult
	}
}

// generator for commit log retention options
func genCommitLogRetention() gopter.Gen {
	return genRetention().
		Map(func(v *generatedRetention) *generatedRetention {
			return &generatedRetention{v.
				SetBufferFuture(0).
				SetBufferPast(0)}
		})
}

type generatedRetention struct {
	retention.Options
}

func (ro *generatedRetention) String() string {
	return fmt.Sprintf(
		"[ retention-period = %v, block-size = %v, buffer-past = %v, buffer-future = %v ]",
		ro.RetentionPeriod().String(),
		ro.BlockSize().String(),
		ro.BufferPast().String(),
		ro.BufferFuture().String())
}

func maxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

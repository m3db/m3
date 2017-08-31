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

package placement

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"

	placementproto "github.com/m3db/m3cluster/generated/proto/placement"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3x/clock"
)

var (
	errNoApplicablePlacement       = errors.New("no applicable placement found")
	errActiveStagedPlacementClosed = errors.New("active staged placement is closed")
)

type activeStagedPlacement struct {
	sync.RWMutex

	placements            []services.Placement
	nowFn                 clock.NowFn
	onPlacementsAddedFn   services.OnPlacementsAddedFn
	onPlacementsRemovedFn services.OnPlacementsRemovedFn

	expiring int32
	closed   bool
	doneFn   services.DoneFn
}

func newActiveStagedPlacement(
	placements []services.Placement,
	opts services.ActiveStagedPlacementOptions,
) services.ActiveStagedPlacement {
	if opts == nil {
		opts = NewActiveStagedPlacementOptions()
	}
	p := &activeStagedPlacement{
		placements:            placements,
		nowFn:                 opts.ClockOptions().NowFn(),
		onPlacementsAddedFn:   opts.OnPlacementsAddedFn(),
		onPlacementsRemovedFn: opts.OnPlacementsRemovedFn(),
	}
	p.doneFn = p.onPlacementDone

	if p.onPlacementsAddedFn != nil {
		p.onPlacementsAddedFn(placements)
	}

	return p
}

func (p *activeStagedPlacement) ActivePlacement() (services.Placement, services.DoneFn, error) {
	p.RLock()
	placement, err := p.activePlacementWithLock(p.nowFn().UnixNano())
	if err != nil {
		p.RUnlock()
		return nil, nil, err
	}
	return placement, p.doneFn, nil
}

func (p *activeStagedPlacement) Close() error {
	p.Lock()
	defer p.Unlock()

	if p.closed {
		return errActiveStagedPlacementClosed
	}
	if p.onPlacementsRemovedFn != nil {
		p.onPlacementsRemovedFn(p.placements)
	}
	p.placements = nil
	return nil
}

func (p *activeStagedPlacement) onPlacementDone() { p.RUnlock() }

func (p *activeStagedPlacement) activePlacementWithLock(timeNanos int64) (services.Placement, error) {
	if p.closed {
		return nil, errActiveStagedPlacementClosed
	}
	idx := p.activeIndexWithLock(timeNanos)
	if idx < 0 {
		return nil, errNoApplicablePlacement
	}
	placement := p.placements[idx]
	// If the placement that's in effect is not the first placment, expire the stale ones.
	if idx > 0 && atomic.CompareAndSwapInt32(&p.expiring, 0, 1) {
		go p.expire()
	}
	return placement, nil
}

// activeIndexWithLock finds the index of the last placement whose cutover time is no
// later than t (a.k.a. the active placement). The cutover times of the placements are
// sorted in ascending order (i.e., earliest time first).
func (p *activeStagedPlacement) activeIndexWithLock(timeNanos int64) int {
	idx := 0
	for idx < len(p.placements) && p.placements[idx].CutoverNanos() <= timeNanos {
		idx++
	}
	idx--
	return idx
}

func (p *activeStagedPlacement) expire() {
	// NB(xichen): this improves readability at the slight cost of lambda capture
	// because this code path is triggered very infrequently.
	cleanup := func() {
		p.Unlock()
		atomic.StoreInt32(&p.expiring, 0)
	}
	p.Lock()
	defer cleanup()

	if p.closed {
		return
	}
	idx := p.activeIndexWithLock(p.nowFn().UnixNano())
	if idx <= 0 {
		return
	}
	if p.onPlacementsRemovedFn != nil {
		p.onPlacementsRemovedFn(p.placements[:idx])
	}
	n := copy(p.placements[0:], p.placements[idx:])
	for i := n; i < len(p.placements); i++ {
		p.placements[i] = nil
	}
	p.placements = p.placements[:n]
}

type stagedPlacement struct {
	version    int
	placements services.Placements
	opts       services.ActiveStagedPlacementOptions
}

// NewStagedPlacement creates an empty staged placement.
func NewStagedPlacement() services.StagedPlacement {
	return &stagedPlacement{}
}

// NewStagedPlacementFromProto creates a new staged placement from proto.
func NewStagedPlacementFromProto(
	version int,
	p *placementproto.PlacementSnapshots,
	opts services.ActiveStagedPlacementOptions,
) (services.StagedPlacement, error) {
	placements, err := NewPlacementsFromProto(p)
	if err != nil {
		return nil, err
	}

	return &stagedPlacement{
		version:    version,
		placements: placements,
		opts:       opts,
	}, nil
}

func (sp *stagedPlacement) ActiveStagedPlacement(timeNanos int64) services.ActiveStagedPlacement {
	idx := len(sp.placements) - 1
	for idx >= 0 && sp.placements[idx].CutoverNanos() > timeNanos {
		idx--
	}
	if idx < 0 {
		return newActiveStagedPlacement(sp.placements, sp.opts)
	}
	return newActiveStagedPlacement(sp.placements[idx:], sp.opts)
}

func (sp *stagedPlacement) Version() int { return sp.version }

func (sp *stagedPlacement) SetVersion(version int) services.StagedPlacement {
	sp.version = version
	return sp
}

func (sp *stagedPlacement) Placements() services.Placements { return sp.placements }

func (sp *stagedPlacement) SetPlacements(placements []services.Placement) services.StagedPlacement {
	sort.Sort(placementsByCutoverAsc(placements))
	sp.placements = placements
	return sp
}

func (sp *stagedPlacement) ActiveStagedPlacementOptions() services.ActiveStagedPlacementOptions {
	return sp.opts
}

func (sp *stagedPlacement) SetActiveStagedPlacementOptions(
	opts services.ActiveStagedPlacementOptions,
) services.StagedPlacement {
	sp.opts = opts
	return sp
}

func (sp *stagedPlacement) Proto() (*placementproto.PlacementSnapshots, error) {
	return sp.Placements().Proto()
}

type placementsByCutoverAsc []services.Placement

func (s placementsByCutoverAsc) Len() int { return len(s) }

func (s placementsByCutoverAsc) Less(i, j int) bool {
	return s[i].CutoverNanos() < s[j].CutoverNanos()
}

func (s placementsByCutoverAsc) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

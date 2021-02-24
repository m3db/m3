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
	"errors"
	"sort"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
)

var (
	errNilPlacementSnapshotsProto = errors.New("nil placement snapshots proto")
	errEmptyPlacementSnapshots    = errors.New("placement snapshots is empty")
)

// Placements represents a list of placements that is backward compatible with
// the deprecated concept of staged placement.
type Placements []Placement

// NewPlacementsFromProto creates a list of placements from proto.
func NewPlacementsFromProto(p *placementpb.PlacementSnapshots) (Placements, error) {
	if p == nil {
		return nil, errNilPlacementSnapshotsProto
	}

	placements := make([]Placement, 0, len(p.Snapshots))
	for _, snapshot := range p.Snapshots {
		placement, err := NewPlacementFromProto(snapshot)
		if err != nil {
			return nil, err
		}
		placements = append(placements, placement)
	}

	// For backward compatibility, we still have to sort
	// if there are actual staged placements.
	sort.Sort(placementsByCutoverAsc(placements))
	return placements, nil
}

// Proto converts a list of Placement to a proto.
func (placements Placements) Proto() (*placementpb.PlacementSnapshots, error) {
	snapshots := make([]*placementpb.Placement, 0, len(placements))
	for _, p := range placements {
		placementProto, err := p.Proto()
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, placementProto)
	}
	return &placementpb.PlacementSnapshots{
		Snapshots: snapshots,
	}, nil
}

// Last returns the last placement from the list.
func (placements Placements) Last() (Placement, error) {
	n := len(placements)
	if n == 0 {
		return nil, errEmptyPlacementSnapshots
	}

	if n > 1 {
		// Sorting for the same reason as mentioned above.
		sort.Sort(placementsByCutoverAsc(placements))
	}

	return placements[n-1], nil
}

type placementsByCutoverAsc []Placement

func (s placementsByCutoverAsc) Len() int { return len(s) }

func (s placementsByCutoverAsc) Less(i, j int) bool {
	return s[i].CutoverNanos() < s[j].CutoverNanos()
}

func (s placementsByCutoverAsc) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

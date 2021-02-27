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
	errNilPlacement               = errors.New("placement is nil")
	errNilPlacementSnapshotsProto = errors.New("PlacementSnapshots proto is nil")
	errEmptyPlacementSnapshots    = errors.New("placement snapshots is empty")
)

// Placements represents a placement that is backward compatible with
// the deprecated concept of staged placement.
type Placements struct {
	latest Placement
}

// NewPlacementsFromLatest creates Placements from latest placement.
func NewPlacementsFromLatest(p Placement) (*Placements, error) {
	if p == nil {
		return nil, errNilPlacement
	}

	return &Placements{
		latest: p,
	}, nil
}

// NewPlacementsFromProto creates Placements from proto.
func NewPlacementsFromProto(p *placementpb.PlacementSnapshots) (*Placements, error) {
	if p == nil {
		return nil, errNilPlacementSnapshotsProto
	}

	if p.CompressMode == placementpb.CompressMode_ZSTD {
		placementProto, err := decompressPlacementProto(p.CompressedPlacement)
		if err != nil {
			return nil, err
		}

		placement, err := NewPlacementFromProto(placementProto)
		if err != nil {
			return nil, err
		}
		return &Placements{
			latest: placement,
		}, nil
	}

	// Fallback to Snapshots field for backward compatibility.
	n := len(p.Snapshots)
	if n == 0 {
		return nil, errEmptyPlacementSnapshots
	}

	all := make([]Placement, 0, n)
	for _, snapshot := range p.Snapshots {
		placement, err := NewPlacementFromProto(snapshot)
		if err != nil {
			return nil, err
		}
		all = append(all, placement)
	}

	// Keep only the latest.
	sort.Sort(placementsByCutoverAsc(all))
	return &Placements{
		latest: all[n-1],
	}, nil
}

// Proto converts Placements to a proto.
func (placements *Placements) Proto() (*placementpb.PlacementSnapshots, error) {
	latest, err := placements.latest.Proto()
	if err != nil {
		return nil, err
	}
	return &placementpb.PlacementSnapshots{
		Snapshots: []*placementpb.Placement{latest},
	}, nil
}

// ProtoCompressed converts Placements to a proto with compressed placement.
func (placements *Placements) ProtoCompressed() (*placementpb.PlacementSnapshots, error) {
	latest, err := placements.latest.Proto()
	if err != nil {
		return nil, err
	}

	compressed, err := compressPlacementProto(latest)
	if err != nil {
		return nil, err
	}

	return &placementpb.PlacementSnapshots{
		CompressMode:        placementpb.CompressMode_ZSTD,
		CompressedPlacement: compressed,
	}, nil
}

// Latest returns the latest placement.
func (placements *Placements) Latest() Placement {
	return placements.latest
}

type placementsByCutoverAsc []Placement

func (s placementsByCutoverAsc) Len() int { return len(s) }

func (s placementsByCutoverAsc) Less(i, j int) bool {
	return s[i].CutoverNanos() < s[j].CutoverNanos()
}

func (s placementsByCutoverAsc) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// Copyright (c) 2016 Uber Technologies, Inc.
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

package etcd

import (
	"fmt"

	schema "github.com/m3db/m3cluster/generated/proto/placement"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
)

const (
	placementPrefix = "_sd.placement"
	metadataPrefix  = "_sd.metadata"
	keyFormat       = "%s/%s"
)

type keyFn func(sid services.ServiceID) string

func placementNamespace(ns string) string {
	if ns == "" {
		ns = placementPrefix
	}

	return ns
}

func metadataNamespace(ns string) string {
	if ns == "" {
		ns = metadataPrefix
	}

	return ns
}

func keyFnWithNamespace(namespace string) keyFn {
	return func(sid services.ServiceID) string {
		return fmt.Sprintf(keyFormat, namespace, serviceKey(sid))
	}
}

func adKey(sid services.ServiceID, id string) string {
	return fmt.Sprintf(keyFormat, serviceKey(sid), id)
}

func serviceKey(s services.ServiceID) string {
	if s.Environment() == "" {
		return s.Name()
	}
	return fmt.Sprintf(keyFormat, s.Environment(), s.Name())
}

func validateServiceID(sid services.ServiceID) error {
	if sid.Name() == "" {
		return errNoServiceName
	}

	return nil
}

func placementProtoFromValue(v kv.Value) (*schema.Placement, error) {
	var placementProto schema.Placement
	if err := v.Unmarshal(&placementProto); err != nil {
		return nil, err
	}

	return &placementProto, nil
}

func placementFromValue(v kv.Value) (services.Placement, error) {
	placementProto, err := placementProtoFromValue(v)
	if err != nil {
		return nil, err
	}

	p, err := placement.NewPlacementFromProto(placementProto)
	if err != nil {
		return nil, err
	}

	return p.SetVersion(v.Version()), nil
}

func placementSnapshotsProtoFromValue(v kv.Value) (*schema.PlacementSnapshots, error) {
	var placementsProto schema.PlacementSnapshots
	if err := v.Unmarshal(&placementsProto); err != nil {
		return nil, err
	}

	return &placementsProto, nil
}

func placementsFromValue(v kv.Value) (services.Placements, error) {
	placementsProto, err := placementSnapshotsProtoFromValue(v)
	if err != nil {
		return nil, err
	}

	ps, err := placement.NewPlacementsFromProto(placementsProto)
	if err != nil {
		return nil, err
	}

	for i, p := range ps {
		ps[i] = p.SetVersion(v.Version())
	}
	return ps, nil
}

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

package client

import (
	"github.com/m3db/m3cluster/services"

	"github.com/golang/protobuf/proto"
)

type storage struct {
	helper placementStorageHelper
	keyFn  keyFn
	kvGen  KVGen
}

func newPlacementStorage(keyFn keyFn, kvGen KVGen, helper placementStorageHelper) services.PlacementStorage {
	return &storage{
		keyFn:  keyFn,
		kvGen:  kvGen,
		helper: helper,
	}
}

func (s *storage) SetPlacementProto(sid services.ServiceID, p proto.Message) error {
	if err := validateServiceID(sid); err != nil {
		return err
	}

	if err := s.helper.ValidateProto(p); err != nil {
		return err
	}

	store, err := s.kvGen(sid.Zone())
	if err != nil {
		return err
	}

	_, err = store.Set(s.keyFn(sid), p)
	return err
}

func (s *storage) PlacementProto(sid services.ServiceID) (proto.Message, int, error) {
	if err := validateServiceID(sid); err != nil {
		return nil, 0, err
	}

	store, err := s.kvGen(sid.Zone())
	if err != nil {
		return nil, 0, err
	}

	return s.helper.PlacementProto(store, s.keyFn(sid))
}

func (s *storage) Set(sid services.ServiceID, p services.Placement) error {
	if err := validateServiceID(sid); err != nil {
		return err
	}

	store, err := s.kvGen(sid.Zone())
	if err != nil {
		return err
	}

	placementProto, err := s.helper.GenerateProto(store, s.keyFn(sid), p)
	if err != nil {
		return err
	}

	_, err = store.Set(s.keyFn(sid), placementProto)
	return err
}

func (s *storage) CheckAndSet(sid services.ServiceID, p services.Placement, version int) error {
	if err := validateServiceID(sid); err != nil {
		return err
	}

	store, err := s.kvGen(sid.Zone())
	if err != nil {
		return err
	}

	placementProto, err := s.helper.GenerateProto(store, s.keyFn(sid), p)
	if err != nil {
		return err
	}

	_, err = store.CheckAndSet(
		s.keyFn(sid),
		version,
		placementProto,
	)
	return err
}

func (s *storage) SetIfNotExist(sid services.ServiceID, p services.Placement) error {
	if err := validateServiceID(sid); err != nil {
		return err
	}

	store, err := s.kvGen(sid.Zone())
	if err != nil {
		return err
	}

	placementProto, err := s.helper.GenerateProto(store, s.keyFn(sid), p)
	if err != nil {
		return err
	}

	_, err = store.SetIfNotExists(
		s.keyFn(sid),
		placementProto,
	)
	return err
}

func (s *storage) Delete(sid services.ServiceID) error {
	if err := validateServiceID(sid); err != nil {
		return err
	}

	store, err := s.kvGen(sid.Zone())
	if err != nil {
		return err
	}

	_, err = store.Delete(s.keyFn(sid))
	return err
}

func (s *storage) Placement(sid services.ServiceID) (services.Placement, int, error) {
	if err := validateServiceID(sid); err != nil {
		return nil, 0, err
	}

	store, err := s.kvGen(sid.Zone())
	if err != nil {
		return nil, 0, err
	}

	return s.helper.Placement(store, s.keyFn(sid))
}

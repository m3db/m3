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

package kv

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/cluster/kv"
	merrors "github.com/m3db/m3/src/metrics/errors"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/rules"
)

var (
	errNilRuleSet    = errors.New("nil ruleset")
	errNilNamespaces = errors.New("nil namespaces")
)

type store struct {
	kvStore kv.TxnStore
	opts    StoreOptions
}

// NewStore creates a new Store.
func NewStore(kvStore kv.TxnStore, opts StoreOptions) rules.Store {
	return &store{kvStore: kvStore, opts: opts}
}

func (s *store) ReadNamespaces() (*rules.Namespaces, error) {
	value, err := s.kvStore.Get(s.opts.NamespacesKey)
	if err != nil {
		return nil, wrapReadError(err)
	}

	version := value.Version()
	var namespaces rulepb.Namespaces
	if err = value.Unmarshal(&namespaces); err != nil {
		return nil, err
	}

	nss, err := rules.NewNamespaces(version, &namespaces)
	if err != nil {
		return nil, err
	}
	return &nss, err
}

func (s *store) ReadRuleSet(nsName string) (rules.RuleSet, error) {
	ruleSetKey := s.ruleSetKey(nsName)
	value, err := s.kvStore.Get(ruleSetKey)
	if err != nil {
		return nil, wrapReadError(err)
	}

	version := value.Version()
	var ruleSet rulepb.RuleSet
	if err = value.Unmarshal(&ruleSet); err != nil {
		return nil, fmt.Errorf("could not fetch RuleSet %s: %v", nsName, err.Error())
	}

	rs, err := rules.NewRuleSetFromProto(version, &ruleSet, rules.NewOptions())
	if err != nil {
		return nil, fmt.Errorf("could not fetch RuleSet %s: %v", nsName, err.Error())
	}
	return rs, err
}

func (s *store) WriteRuleSet(rs rules.MutableRuleSet) error {
	if s.opts.Validator != nil {
		if err := s.opts.Validator.Validate(rs); err != nil {
			return err
		}
	}
	rsCond, rsOp, err := s.ruleSetTransaction(rs)
	if err != nil {
		return err
	}
	conditions, ops := []kv.Condition{rsCond}, []kv.Op{rsOp}
	_, err = s.kvStore.Commit(conditions, ops)
	return wrapWriteError(err)
}

func (s *store) WriteNamespaces(nss *rules.Namespaces) error {
	namespacesCond, namespacesOp, err := s.namespacesTransaction(nss)
	if err != nil {
		return err
	}
	conditions, ops := []kv.Condition{namespacesCond}, []kv.Op{namespacesOp}
	_, err = s.kvStore.Commit(conditions, ops)
	return wrapWriteError(err)
}

func (s *store) WriteAll(nss *rules.Namespaces, rs rules.MutableRuleSet) error {
	if s.opts.Validator != nil {
		if err := s.opts.Validator.Validate(rs); err != nil {
			return err
		}
	}

	var (
		conditions []kv.Condition
		ops        []kv.Op
	)
	ruleSetCond, ruleSetOp, err := s.ruleSetTransaction(rs)
	if err != nil {
		return err
	}
	conditions = append(conditions, ruleSetCond)
	ops = append(ops, ruleSetOp)

	namespacesCond, namespacesOp, err := s.namespacesTransaction(nss)
	if err != nil {
		return err
	}
	conditions = append(conditions, namespacesCond)
	ops = append(ops, namespacesOp)
	_, err = s.kvStore.Commit(conditions, ops)
	return wrapWriteError(err)
}

func (s *store) Close() {
	if s.opts.Validator == nil {
		return
	}
	s.opts.Validator.Close()
}

func (s *store) ruleSetKey(ns string) string {
	return fmt.Sprintf(s.opts.RuleSetKeyFmt, ns)
}

func (s *store) ruleSetTransaction(rs rules.MutableRuleSet) (kv.Condition, kv.Op, error) {
	if rs == nil {
		return nil, nil, errNilRuleSet
	}

	ruleSetKey := s.ruleSetKey(string(rs.Namespace()))
	rsProto, err := rs.Proto()
	if err != nil {
		return nil, nil, err
	}

	ruleSetCond := kv.NewCondition().
		SetKey(ruleSetKey).
		SetCompareType(kv.CompareEqual).
		SetTargetType(kv.TargetVersion).
		SetValue(rs.Version())

	return ruleSetCond, kv.NewSetOp(ruleSetKey, rsProto), nil
}

func (s *store) namespacesTransaction(nss *rules.Namespaces) (kv.Condition, kv.Op, error) {
	if nss == nil {
		return nil, nil, errNilNamespaces
	}

	namespacesKey := s.opts.NamespacesKey
	nssProto, err := nss.Proto()
	if err != nil {
		return nil, nil, err
	}
	namespacesCond := kv.NewCondition().
		SetKey(namespacesKey).
		SetCompareType(kv.CompareEqual).
		SetTargetType(kv.TargetVersion).
		SetValue(nss.Version())

	return namespacesCond, kv.NewSetOp(namespacesKey, nssProto), nil
}

func wrapWriteError(err error) error {
	if err == kv.ErrConditionCheckFailed {
		return merrors.NewStaleDataError(
			fmt.Sprintf("stale write request: %s", err.Error()),
		)
	}

	return err
}

func wrapReadError(err error) error {
	switch err {
	case kv.ErrNotFound:
		return merrors.NewNotFoundError(
			fmt.Sprintf("not found: %s", err.Error()),
		)
	default:
		return err
	}
}

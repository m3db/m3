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

package policy

import (
	"errors"
	"fmt"
	"strings"
	"time"

	schema "github.com/m3db/m3metrics/generated/proto/policypb"
	xtime "github.com/m3db/m3x/time"
)

const (
	resolutionRetentionSeparator = ":"
)

var (
	// EmptyStoragePolicy represents an empty storage policy.
	EmptyStoragePolicy StoragePolicy

	errNilStoragePolicySchema     = errors.New("nil storage policy schema")
	errInvalidStoragePolicyString = errors.New("invalid storage policy string")
)

// StoragePolicy represents the resolution and retention period metric datapoints
// are stored at.
type StoragePolicy struct {
	resolution Resolution
	retention  Retention
}

// NewStoragePolicy creates a new storage policy given a resolution and a retention.
func NewStoragePolicy(window time.Duration, precision xtime.Unit, retention time.Duration) StoragePolicy {
	return StoragePolicy{
		resolution: Resolution{
			Window:    window,
			Precision: precision,
		},
		retention: Retention(retention),
	}
}

// NewStoragePolicyFromProto creates a new storage policy from a storage policy protobuf message.
func NewStoragePolicyFromProto(p *schema.StoragePolicy) (StoragePolicy, error) {
	if p == nil {
		return EmptyStoragePolicy, errNilStoragePolicySchema
	}
	precision := time.Duration(p.Resolution.Precision)
	unit, err := xtime.UnitFromDuration(precision)
	if err != nil {
		return EmptyStoragePolicy, err
	}

	return NewStoragePolicy(time.Duration(p.Resolution.WindowSize), unit, time.Duration(p.Retention.Period)), nil
}

// String is the string representation of a storage policy.
func (p StoragePolicy) String() string {
	return fmt.Sprintf("%s%s%s", p.resolution.String(), resolutionRetentionSeparator, p.retention.String())
}

// Resolution returns the resolution of the storage policy.
func (p StoragePolicy) Resolution() Resolution {
	return p.resolution
}

// Retention return the retention of the storage policy.
func (p StoragePolicy) Retention() Retention {
	return p.retention
}

// ToProto converts the storage policy to a protobuf message in place.
func (p StoragePolicy) ToProto(pb *schema.StoragePolicy) error {
	if pb.Resolution == nil {
		pb.Resolution = &schema.Resolution{}
	}
	if err := p.resolution.ToProto(pb.Resolution); err != nil {
		return err
	}
	if pb.Retention == nil {
		pb.Retention = &schema.Retention{}
	}
	p.retention.ToProto(pb.Retention)
	return nil
}

// FromProto converts the protobuf message to a storage policy in place.
func (p *StoragePolicy) FromProto(pb schema.StoragePolicy) error {
	if err := p.resolution.FromProto(pb.Resolution); err != nil {
		return err
	}
	if err := p.retention.FromProto(pb.Retention); err != nil {
		return err
	}
	return nil
}

// UnmarshalYAML unmarshals a storage policy value from a string.
func (p *StoragePolicy) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	parsed, err := ParseStoragePolicy(str)
	if err != nil {
		return err
	}
	*p = parsed
	return nil
}

// ParseStoragePolicy parses a storage policy in the form of resolution:retention.
func ParseStoragePolicy(str string) (StoragePolicy, error) {
	parts := strings.Split(str, resolutionRetentionSeparator)
	if len(parts) != 2 {
		return EmptyStoragePolicy, errInvalidStoragePolicyString
	}
	resolution, err := ParseResolution(parts[0])
	if err != nil {
		return EmptyStoragePolicy, err
	}
	retention, err := ParseRetention(parts[1])
	if err != nil {
		return EmptyStoragePolicy, err
	}
	return StoragePolicy{resolution: resolution, retention: retention}, nil
}

// MustParseStoragePolicy parses a storage policy in the form of resolution:retention,
// and panics if the input string is invalid.
func MustParseStoragePolicy(str string) StoragePolicy {
	sp, err := ParseStoragePolicy(str)
	if err != nil {
		panic(fmt.Errorf("invalid storage policy string %s: %v", str, err))
	}
	return sp
}

// IsDefaultStoragePolicies returns whether a list of storage policies are considered
// as default storage policies.
func IsDefaultStoragePolicies(policies []StoragePolicy) bool {
	return len(policies) == 0
}

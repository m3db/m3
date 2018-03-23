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

package namespace

import (
	"fmt"

	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3x/ident"
)

// MapConfiguration is the configuration for a registry of namespaces
type MapConfiguration struct {
	Metadatas []MetadataConfiguration `yaml:"metadatas" validate:"nonzero"`
}

// MetadataConfiguration is the configuration for a single namespace
type MetadataConfiguration struct {
	ID                string                  `yaml:"id" validate:"nonzero"`
	NeedsBootstrap    *bool                   `yaml:"needsBootstrap"`
	NeedsFlush        *bool                   `yaml:"needsFlush"`
	WritesToCommitLog *bool                   `yaml:"writesToCommitLog"`
	NeedsCleanup      *bool                   `yaml:"needsCleanup"`
	NeedsRepair       *bool                   `yaml:"needsRepair"`
	Retention         retention.Configuration `yaml:"retention" validate:"nonzero"`
}

// Map returns a Map corresponding to the receiver struct
func (m *MapConfiguration) Map() (Map, error) {
	metadatas := make([]Metadata, 0, len(m.Metadatas))
	for _, m := range m.Metadatas {
		md, err := m.Metadata()
		if err != nil {
			return nil, fmt.Errorf("unable to construct metadata for [%+v], err: %v", m, err)
		}
		metadatas = append(metadatas, md)
	}
	return NewMap(metadatas)
}

// Metadata returns a Metadata corresponding to the receiver struct
func (mc *MetadataConfiguration) Metadata() (Metadata, error) {
	ropts := mc.Retention.Options()
	opts := NewOptions().SetRetentionOptions(ropts)
	if v := mc.NeedsBootstrap; v != nil {
		opts = opts.SetNeedsBootstrap(*v)
	}
	if v := mc.NeedsFlush; v != nil {
		opts = opts.SetFlushEnabled(*v)
	}
	if v := mc.WritesToCommitLog; v != nil {
		opts = opts.SetWritesToCommitLog(*v)
	}
	if v := mc.NeedsCleanup; v != nil {
		opts = opts.SetCleanupEnabled(*v)
	}
	if v := mc.NeedsRepair; v != nil {
		opts = opts.SetRepairEnabled(*v)
	}
	return NewMetadata(ident.StringID(mc.ID), opts)
}

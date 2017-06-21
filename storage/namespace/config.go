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
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/ts"
)

// RegistryConfiguration is the configuration for a registry of namespaces
type RegistryConfiguration struct {
	Metadatas []MetadataConfiguration `yaml:"metadata" validate:"nonzero"`
}

// MetadataConfiguration is the configuration for a single namespace
type MetadataConfiguration struct {
	ID                  string                   `yaml:"id" validate:"nonzero"`
	NeedsBootstrap      *bool                    `yaml:"needsBootstrap"`
	NeedsFlush          *bool                    `yaml:"needsFlush"`
	WritesToCommitLog   *bool                    `yaml:"writesToCommitLog"`
	NeedsFilesetCleanup *bool                    `yaml:"needsFilesetCleanup"`
	NeedsRepair         *bool                    `yaml:"needsRepair"`
	Retention           *retention.Configuration `yaml:"retention"`
}

// Registry returns a Registry corresponding to the receiver struct
func (rc *RegistryConfiguration) Registry() Registry {
	metadatas := make([]Metadata, 0, len(rc.Metadatas))
	for _, m := range rc.Metadatas {
		metadatas = append(metadatas, m.Metadata())
	}
	return NewRegistry(metadatas)
}

// Metadata returns a Metadata corresponding to the receiver struct
func (mc *MetadataConfiguration) Metadata() Metadata {
	opts := NewOptions()
	if v := mc.NeedsBootstrap; v != nil {
		opts = opts.SetNeedsBootstrap(*v)
	}
	if v := mc.NeedsFlush; v != nil {
		opts = opts.SetNeedsFlush(*v)
	}
	if v := mc.WritesToCommitLog; v != nil {
		opts = opts.SetWritesToCommitLog(*v)
	}
	if v := mc.NeedsFilesetCleanup; v != nil {
		opts = opts.SetNeedsFilesetCleanup(*v)
	}
	if v := mc.NeedsRepair; v != nil {
		opts = opts.SetNeedsRepair(*v)
	}
	if v := mc.Retention; v != nil {
		opts = opts.SetRetentionOptions(v.Options())
	}
	return NewMetadata(ts.StringID(mc.ID), opts)
}

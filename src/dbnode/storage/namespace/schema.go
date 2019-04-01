// Copyright (c) 2019 Uber Technologies, Inc.
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
	"bytes"
	"compress/gzip"
	"errors"
	"io/ioutil"
	"sort"

	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
)

var (
	errInvalidSchema  = errors.New("invalid schema definition")
	errSchemaNotFound = errors.New("schema is not found")
)

type schemaDescr struct {
	id      ident.ID
	version uint32
	md      *desc.MessageDescriptor
}

func (s *schemaDescr) ID() ident.ID {
	return s.id
}

func (s *schemaDescr) Version() uint32 {
	return s.version
}

func (s *schemaDescr) Equal(o SchemaDescr) bool {
	if s == nil && o == nil {
		return true
	}
	if s != nil && o == nil || s == nil && o != nil {
		return false
	}
	return s.ID().String() == o.ID().String() && s.Version() == o.Version()
}

func (s *schemaDescr) Get() *desc.MessageDescriptor {
	return s.md
}

func (s *schemaDescr) String() string {
	if s.md == nil {
		return ""
	}
	return s.md.String()
}

type schemaRegistry struct {
	options *nsproto.SchemaOptions
	schemas map[string]*schemaDescr
}

func (sr *schemaRegistry) Equal(o SchemaRegistry) bool {
	var osr *schemaRegistry
	var ok bool
	if sr == nil && o == nil {
		return true
	}
	if sr != nil && o == nil || sr == nil && o != nil {
		return false
	}

	if osr, ok = o.(*schemaRegistry); !ok {
		return false
	}
	if len(sr.schemas) != len(osr.schemas) {
		return false
	}
	for n, sd := range sr.schemas {
		var osd *schemaDescr
		if osd, ok = osr.schemas[n]; !ok {
			return false
		}
		if !sd.Equal(osd) {
			return false
		}
	}
	return true
}

func (sr *schemaRegistry) Get(id ident.ID) (SchemaDescr, error) {
	sd, ok := sr.schemas[id.String()]
	if !ok {
		return nil, errSchemaNotFound
	}
	return sd, nil
}

func (sr *schemaRegistry) IDs() []ident.ID {
	ids := make([]ident.ID, len(sr.schemas))
	i := 0
	for id := range sr.schemas {
		ids[i] = ident.StringID(id)
		i++
	}
	return ids
}

// toSchemaOptions returns the corresponding SchemaOptions proto for the provided SchemaRegistry
func toSchemaOptions(sr SchemaRegistry) *nsproto.SchemaOptions {
	if sr == nil {
		return nil
	}
	_, ok := sr.(*schemaRegistry)
	if !ok {
		return nil
	}
	return sr.(*schemaRegistry).options
}

func emptySchemaRegistry() SchemaRegistry {
	return &schemaRegistry{options: nil, schemas: make(map[string]*schemaDescr)}
}

// LoadSchemaRegistry loads schema registry from SchemaOptions proto.
func LoadSchemaRegistry(options *nsproto.SchemaOptions) (SchemaRegistry, error) {
	if options == nil ||
		options.GetHistory() == nil ||
		len(options.GetHistory().GetVersions()) == 0 ||
		len(options.GetSchemas()) == 0 {
		return &schemaRegistry{options: options, schemas: make(map[string]*schemaDescr)}, nil
	}

	// sorted by version in descending order (most recent first)
	versions := options.GetHistory().GetVersions()
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].Version > versions[j].Version
	})

	// take the most recent version
	fdbSet := versions[0].Descriptors
	version := versions[0].Version

	// assuming file descriptors are topological sorted
	var dependencies []*desc.FileDescriptor
	msgDescr := make(map[string]*desc.MessageDescriptor)
	for i, fdb := range fdbSet {
		fdp, err := decodeProtoFileDescriptor(fdb)
		if err != nil {
			return nil, xerrors.Wrapf(err, "failed to decode file descriptor(%d) in version(%d)", i, version)
		}
		fd, err := desc.CreateFileDescriptor(fdp, dependencies...)
		if err != nil {
			return nil, xerrors.Wrapf(err, "failed to create file descriptor(%d) in version(%d)", i, version)
		}
		dependencies = append(dependencies, fd)

		for _, md := range fd.GetMessageTypes() {
			msgDescr[md.GetName()] = md
		}
	}

	sr := &schemaRegistry{options: options, schemas: make(map[string]*schemaDescr)}
	for n, sm := range options.GetSchemas() {
		md, ok := msgDescr[sm.GetMessageName()]
		if !ok {
			return nil, xerrors.Wrapf(errInvalidSchema, "message %s is not found in version(%d)", sm.MessageName, version)
		}
		sr.schemas[n] = &schemaDescr{id: ident.StringID(n), version: version, md: md}
	}
	return sr, nil
}

// decodeProtoFileDescriptor decodes the bytes of proto file descriptor.
// proto file descriptor is proto encoded and gzipped, decode reverse the process.
func decodeProtoFileDescriptor(fdb []byte) (*dpb.FileDescriptorProto, error) {
	raw, err := decompress(fdb)
	if err != nil {
		return nil, err
	}
	fd := dpb.FileDescriptorProto{}

	if err := proto.Unmarshal(raw, &fd); err != nil {
		return nil, err
	}
	return &fd, nil
}

func decompress(b []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	out, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return out, nil
}

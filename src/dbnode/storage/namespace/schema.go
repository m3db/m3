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

	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	xerrors "github.com/m3db/m3x/errors"
	"sort"
	"strings"
)

var (
	errInvalidSchema = errors.New("invalid schema")
)

type schema struct {
	fdp []byte
	md  *desc.MessageDescriptor
	version uint32
}

func (s *schema) Version() uint32 {
	return s.version
}

func (s *schema) Equal(o Schema) bool {
	return s.Version() == o.Version() && bytes.Equal(s.Bytes(), o.Bytes())
}

func (s *schema) Get() *desc.MessageDescriptor {
	return s.md
}

func (s *schema) String() string {
	if s.md == nil {
		return ""
	}
	return s.md.String()
}

func (s *schema) Bytes() []byte {
	return s.fdp
}

func schemaListEqual(l, r []Schema) bool {
	if len(l) != len(r) {
		return false
	}
	for i := 0; i < len(l); i++ {
		if !l[i].Equal(r[i]) {
			return false
		}
	}
	return true
}

func schemaToOption (s Schema) *nsproto.SchemaOption {
	return &nsproto.SchemaOption{Version: s.Version(), Message: s.Get().GetName(), Definition: s.Bytes()}
}

func fromSchemaList(schemaList []Schema) []*nsproto.SchemaOption {
	result := make([]*nsproto.SchemaOption, len(schemaList))
	for i := 0; i < len(schemaList); i++ {
		result[i] = schemaToOption(schemaList[i])
	}
	return result
}

// ToSchemaList converts schema options to an array of schema, most recent schema version first.
func ToSchemaList(options []*nsproto.SchemaOption) ([]Schema, error) {
	var result []Schema
	for _, o := range options {
		s, err := ToSchema(o)
		if err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	// sorted by version in decending order
	sort.Slice(result, func(i, j int) bool {
		return result[i].Version() > result[j].Version()
	})
	return result, nil
}

func ToSchema(o *nsproto.SchemaOption) (Schema, error) {
	if o == nil {
		return nil, nil
	}

	fdp, err := decodeProtoFileDescriptor(o.Definition)
	if err != nil {
		return nil, xerrors.Wrap(err, "failed to decode schema file descriptor")
	}

	fd, err := desc.CreateFileDescriptor(fdp)
	if err != nil {
		return nil, xerrors.Wrap(err, "failed to create schema file descriptor")
	}
	mds := fd.GetMessageTypes()
	if len(mds) == 0 {
		return nil, xerrors.Wrap(errInvalidSchema, "schema does not contain any messages")
	}

	var found *desc.MessageDescriptor
	for _, md := range mds {
		if strings.EqualFold(md.GetName(), o.Message) {
			found = md
		}
	}
	if found == nil {
		return nil, xerrors.Wrapf(errInvalidSchema, "schema does not contain %v", o.Message)
	}

	return &schema{fdp: o.Definition, md: found, version: o.Version}, nil
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

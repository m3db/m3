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
	"errors"

	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
)

var (
	errInvalidSchema        = errors.New("invalid schema definition")
	errSchemaRegistryEmpty  = errors.New("schema registry is empty")
	errInvalidSchemaOptions = errors.New("invalid schema options")
	errEmptyProtoFile       = errors.New("empty proto file")
	errSyntaxNotProto3      = errors.New("proto syntax is not proto3")
)

type schemaDescr struct {
	deployId string
	md       *desc.MessageDescriptor
}

func (s *schemaDescr) DeployId() string {
	return s.deployId
}

func (s *schemaDescr) Equal(o SchemaDescr) bool {
	if s == nil && o == nil {
		return true
	}
	if s != nil && o == nil || s == nil && o != nil {
		return false
	}
	if _, ok := o.(*schemaDescr); !ok {
		return false
	}
	return s.DeployId() == o.DeployId()
}

type MessageDescriptor struct {
	*desc.MessageDescriptor
}

func (s *schemaDescr) Get() MessageDescriptor {
	return MessageDescriptor{s.md}
}

func (s *schemaDescr) String() string {
	if s.md == nil {
		return ""
	}
	return s.md.String()
}

type schemaRegistry struct {
	options  *nsproto.SchemaOptions
	latestId string
	// a map of schema version to schema descriptor.
	versions map[string]*schemaDescr
}

func (sr *schemaRegistry) Equal(o SchemaRegistry) bool {
	var osr *schemaRegistry
	var ok bool

	if osr, ok = o.(*schemaRegistry); !ok {
		return false
	}
	// compare latest version
	if sr.latestId != osr.latestId {
		return false
	}

	// compare version map
	if len(sr.versions) != len(osr.versions) {
		return false
	}
	for v, sd := range sr.versions {
		osd, ok := osr.versions[v]
		if !ok {
			return false
		}
		if !sd.Equal(osd) {
			return false
		}
	}

	return true
}

func (sr *schemaRegistry) Get(id string) (SchemaDescr, bool) {
	sd, ok := sr.versions[id]
	if !ok {
		return nil, false
	}
	return sd, true
}

func (sr *schemaRegistry) GetLatest() (SchemaDescr, bool) {
	return sr.Get(sr.latestId)
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
	return &schemaRegistry{options: nil, versions: make(map[string]*schemaDescr)}
}

// LoadSchemaRegistry loads schema registry from SchemaOptions proto.
func LoadSchemaRegistry(options *nsproto.SchemaOptions) (SchemaRegistry, error) {
	sr := &schemaRegistry{options: options, versions: make(map[string]*schemaDescr)}
	if options == nil ||
		options.GetHistory() == nil ||
		len(options.GetHistory().GetVersions()) == 0 {
		return sr, nil
	}

	msgName := options.GetDefaultMessageName()
	if len(msgName) == 0 {
		return nil, xerrors.Wrap(errInvalidSchemaOptions, "default message name is not specified")
	}

	var prevId string
	for _, fdbSet := range options.GetHistory().GetVersions() {
		if len(prevId) > 0 && fdbSet.PrevId != prevId {
			return nil, xerrors.Wrapf(errInvalidSchemaOptions, "schema history is not sorted by deploy id in ascending order")
		}
		sd, err := loadFileDescriptorSet(fdbSet, msgName)
		if err != nil {
			return nil, err
		}
		sr.versions[sd.DeployId()] = sd
		prevId = sd.DeployId()
	}
	sr.latestId = prevId

	return sr, nil
}

func loadFileDescriptorSet(fdSet *nsproto.FileDescriptorSet, msgName string) (*schemaDescr, error) {
	// assuming file descriptors are topological sorted
	var dependencies []*desc.FileDescriptor
	var curfd *desc.FileDescriptor
	for i, fdb := range fdSet.Descriptors {
		fdp, err := decodeFileDescriptorProto(fdb)
		if err != nil {
			return nil, xerrors.Wrapf(err, "failed to decode file descriptor(%d) in version(%s)", i, fdSet.DeployId)
		}
		fd, err := desc.CreateFileDescriptor(fdp, dependencies...)
		if err != nil {
			return nil, xerrors.Wrapf(err, "failed to create file descriptor(%d) in version(%s)", i, fdSet.DeployId)
		}
		if !fd.IsProto3() {
			return nil, xerrors.Wrapf(errSyntaxNotProto3, "file descriptor(%s) is not proto3", fd.GetFullyQualifiedName())
		}
		curfd = fd
		dependencies = append(dependencies, curfd)
	}

	md := curfd.FindMessage(msgName)
	if md != nil {
		return &schemaDescr{deployId: fdSet.DeployId, md: md}, nil
	}
	return nil, xerrors.Wrapf(errInvalidSchemaOptions, "failed to find message (%s) in version(%s)", msgName, fdSet.DeployId)
}

// decodeFileDescriptorProto decodes the bytes of proto file descriptor.
func decodeFileDescriptorProto(fdb []byte) (*dpb.FileDescriptorProto, error) {
	fd := dpb.FileDescriptorProto{}

	if err := proto.Unmarshal(fdb, &fd); err != nil {
		return nil, err
	}
	return &fd, nil
}

// genDependencyDescriptors produces a topological sort of the dependency descriptors for the provided
// file descriptor, the result contains the input file descriptor as the last in the slice,
// the result contains indirect dependencies as well, dependencies in the return are distinct.
func genDependencyDescriptors(infd *desc.FileDescriptor) []*desc.FileDescriptor {
	var depfds []*desc.FileDescriptor
	dedup := make(map[string]struct{})

	for _, dep := range infd.GetDependencies() {
		depfs2 := genDependencyDescriptors(dep)
		for _, fd := range depfs2 {
			if _, ok := dedup[fd.GetFullyQualifiedName()]; !ok {
				dedup[fd.GetFullyQualifiedName()] = struct{}{}
				depfds = append(depfds, fd)
			}
		}
	}
	if _, ok := dedup[infd.GetFullyQualifiedName()]; !ok {
		depfds = append(depfds, infd)
		dedup[infd.GetFullyQualifiedName()] = struct{}{}
	}
	return depfds
}

func parseProto(protoFile string, importPaths ...string) ([]*desc.FileDescriptor, error) {
	p := protoparse.Parser{ImportPaths: importPaths, IncludeSourceCodeInfo: true}
	fds, err := p.ParseFiles(protoFile)
	if err != nil {
		return nil, xerrors.Wrapf(err, "failed to parse proto file: %s", protoFile)
	}
	if len(fds) == 0 {
		return nil, xerrors.Wrapf(errEmptyProtoFile, "proto file (%s) can not be parsed", protoFile)
	}
	if !fds[0].IsProto3() {
		return nil, xerrors.Wrapf(errSyntaxNotProto3, "proto file (%s) is not proto3", protoFile)
	}
	return genDependencyDescriptors(fds[0]), nil
}

func marshalFileDescriptors(fdList []*desc.FileDescriptor) ([][]byte, error) {
	var dlist [][]byte
	for _, fd := range fdList {
		fdbytes, err := proto.Marshal(fd.AsProto())
		if err != nil {
			return nil, xerrors.Wrapf(err, "failed to marshal file descriptor: %s", fd.GetFullyQualifiedName())
		}
		dlist = append(dlist, fdbytes)
	}
	return dlist, nil
}

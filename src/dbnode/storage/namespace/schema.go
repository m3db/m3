package namespace

import (
	"errors"
	"fmt"
	"compress/gzip"
	"bytes"
	"io/ioutil"

	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
)

var (
	errInvalidSchema = errors.New("invalid schema")
)

type schema struct {
	fdp []byte
	md *desc.MessageDescriptor
}

func NewSchema() Schema {
	return &schema{}
}

func (s *schema) Equal(o Schema) bool {
	return bytes.Equal(s.Bytes(), o.Bytes())
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

func ToSchema(schemaBytes []byte) (Schema, error) {
	if schemaBytes == nil {
		return NewSchema(), nil
	}

	fdp, err := decodeFileDescriptor("schema", schemaBytes)
	if err != nil {
		return nil, err
	}

	fd, err := desc.CreateFileDescriptor(fdp)
	if err != nil {
		return nil, err
	}
	if len(fd.GetMessageTypes()) == 0 {
		return nil, errInvalidSchema
	}

	return &schema{fdp: schemaBytes, md: fd.GetMessageTypes()[0]}, nil
}

// DecodeFileDescriptor decodes the bytes of a registered file descriptor.
// Registered file descriptors are first "proto encoded" (e.g. binary format
// for the descriptor protos) and then gzipped. So this function gunzips and
// then unmarshals into a descriptor proto.
func decodeFileDescriptor(element string, fdb []byte) (*dpb.FileDescriptorProto, error) {
	raw, err := decompress(fdb)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress %q descriptor: %v", element, err)
	}
	fd := dpb.FileDescriptorProto{}

	if err := proto.Unmarshal(raw, &fd); err != nil {
		return nil, fmt.Errorf("bad descriptor for %q: %v", element, err)
	}
	return &fd, nil
}

func decompress(b []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("bad gzipped descriptor: %v", err)
	}
	out, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("bad gzipped descriptor: %v", err)
	}
	return out, nil
}

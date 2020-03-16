package yaml

import (
	"bytes"
	"go.uber.org/zap"
	"io"
	"io/ioutil"

	"github.com/ghodss/yaml"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
)

// this reads a yaml representation of an m3 structure
// and produces an io.Reader of it protocol buffer-encoded
//
// we don't know anything about what the user it trying to do
// so peek at it to see what's the intended action then load it
//
// See the examples directories.
//
func Load(path string, zl *zap.Logger) (string, io.Reader, error) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return "", nil, err
	}
	url, pbmessage, err := peeker(content)
	if err != nil {
		return "", nil, err
	}
	rv, err := _load(content, pbmessage)
	return url, rv, nil
}

func _load(content []byte, target proto.Message) (io.Reader, error) {
	// unmarshal it into json
	if err := yaml.Unmarshal(content, target); err != nil {
		return nil, err
	}
	// marshal it into protocol buffers
	var data *bytes.Buffer
	data = bytes.NewBuffer(nil)
	marshaller := &jsonpb.Marshaler{}
	if err := marshaller.Marshal(data, target); err != nil {
		return nil, err
	}
	return data, nil
}

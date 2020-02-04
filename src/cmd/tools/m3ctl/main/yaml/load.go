package yaml

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"

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
func Load(path string) (string, io.Reader) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	url, pbmessage, err := peeker(content)
	if err != nil {
		log.Fatalf("error inspecting the yaml:it might be bad yaml or it might be an unknown operation:%v:from yaml file:%s:", err, path)
	}

	rv, err :=  _load(content, pbmessage)

	return url, rv
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

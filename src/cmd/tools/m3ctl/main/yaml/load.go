package yaml

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"

	pb "github.com/m3db/m3/src/cmd/tools/m3ctl/main/yaml/generated"

	"github.com/ghodss/yaml"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
)

// this reads a yaml representation of an m3 structure
// and produces an io.Reader of it protocol buffer-encoded
//
// we don't know anything about what the user it trying to do
// but we only know how to do the things that are the keys of
// this map DatabaseCreateRequestYaml_OperationType_value
// so try to load each and take whatever works
// there's a "operation" field the user must specify to
// indicate the intent
//
// See the examples directories.
//
// the strategy is to loop over the keys of
// DatabaseCreateRequestYaml_OperationType_value
// and try to load for each
// checking the "operation" field
func Load(path string, target proto.Message) io.Reader {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	rv, err := _load(content, target)
	if err != nil {
		log.Fatalf("cannot unmarshal data:%v:from yaml file:%s:", err, path)
	}
	return rv
}

func q() (io.Reader, error) {

	for k := range pb.DatabaseCreateRequestYaml_OperationType_value {

	}
}

// more easily testable version
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

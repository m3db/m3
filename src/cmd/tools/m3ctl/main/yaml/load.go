package yaml

import (
	"bytes"
	"github.com/ghodss/yaml"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
)

func Load(path string, target proto.Message, log *zap.SugaredLogger) io.Reader {
	log.Debugf("path:%s:\n", path)
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

// more easily testable version
func _load(content []byte, target proto.Message) (io.Reader, error) {
	if err := yaml.Unmarshal(content, target); err != nil {
		return nil, err
	}
	var data *bytes.Buffer
	data = bytes.NewBuffer(nil)
	marshaller := &jsonpb.Marshaler{}
	if err := marshaller.Marshal(data, target); err != nil {
		return nil, err
	}
	return data, nil
}

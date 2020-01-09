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
	if err = yaml.Unmarshal(content, target); err != nil {
		log.Fatalf("cannot unmarshal data:%v:from yaml file:%s:", err, path)
	}
	var data *bytes.Buffer
	data = bytes.NewBuffer(nil)
	marshaller := &jsonpb.Marshaler{}
	if err = marshaller.Marshal(data, target); err != nil {
		log.Fatal(err)
	}
	return data
}

package yaml

import (
	"io/ioutil"
	"testing"

	"github.com/gogo/protobuf/jsonpb"
	pb "github.com/m3db/m3/src/cmd/tools/m3ctl/yaml/generated"
)

// this uses _load to get an encoded stream of the
// structure, then unmarshals it back into a struct
// and verifies the unmarshalled struct matches
// what was specified in the yaml
func TestLoadBasic(t *testing.T) {
	content, err := ioutil.ReadFile("./testdata/basicCreate.yaml")
	if err != nil {
		t.Fatalf("failed to read yaml test data:%v:\n", err)
	}
	// load the yaml and encode it
	source := pb.DatabaseCreateRequestYaml{}
	data, err := _load(content, &source)
	if err != nil {
		t.Fatalf("failed to encode the basic test data:%v:\n", err)
	}
	dest := pb.DatabaseCreateRequestYaml{}
	unmarshaller := &jsonpb.Unmarshaler{AllowUnknownFields: true}
	// unmarshal the stream back into a struct and verify it
	if err := unmarshaller.Unmarshal(data, &dest); err != nil {
		t.Fatalf("failed to unmarshal basic test data:%v:\n", err)
	}
	if dest.Operation != opCreate {
		t.Errorf("dest type does not have the correct type:expected:%v:got:%v:", opCreate, dest.Operation)
	}
	if dest.Request.ReplicationFactor != 327 {
		t.Errorf("in and out ReplicationFactor did not match:expected:%d:got:%d:\n", source.Request.ReplicationFactor, dest.Request.ReplicationFactor)
	}
	if dest.Request.ReplicationFactor != source.Request.ReplicationFactor {
		t.Errorf("in and out ReplicationFactor did not match:expected:%d:got:%d:\n", source.Request.ReplicationFactor, dest.Request.ReplicationFactor)
	}
	if dest.Request.NamespaceName != "default" {
		t.Errorf("namespace is wrong:expected:%s:got:%s:\n", "default", dest.Request.NamespaceName)
	}
	if len(dest.Request.Hosts) != 1 {
		t.Errorf("number of hosts is wrong:expected:%d:got:%d:\n", 1, len(dest.Request.Hosts))
	}
	if dest.Request.Hosts[0].Id != "m3db_seed" {
		t.Errorf("hostname is wrong:expected:%s:got:%s:\n", "m3db_seed", dest.Request.Hosts[0])
	}
}

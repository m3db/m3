package yaml

import (
	"io/ioutil"
	"testing"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/m3db/m3/src/query/generated/proto/admin"
)

func TestLoadBasic(t *testing.T) {
	content, err := ioutil.ReadFile("./testdata/basicCreate.yaml")
	if err != nil {
		t.Fatalf("failed to read yaml test data:%v:\n", err)
	}

	source := admin.DatabaseCreateRequest{}
	data, err := _load(content, &source)

	dest := admin.DatabaseCreateRequest{}
	unmarshaller := &jsonpb.Unmarshaler{AllowUnknownFields: true}
	if err := unmarshaller.Unmarshal(data, &dest); err != nil {
		t.Fatalf("failed to unmarshal basic test data:%v:\n", err)
	}
	if dest.ReplicationFactor != 327 {
		t.Errorf("in and out ReplicationFactor did not match:expected:%d:got:%d:\n", source.ReplicationFactor, dest.ReplicationFactor)
	}
	if dest.ReplicationFactor != source.ReplicationFactor {
		t.Errorf("in and out ReplicationFactor did not match:expected:%d:got:%d:\n", source.ReplicationFactor, dest.ReplicationFactor)
	}
	if dest.NamespaceName != "default" {
		t.Errorf("namespace is wrong:expected:%s:got:%s:\n", "default", dest.NamespaceName)
	}
	if len(dest.Hosts) != 1 {
		t.Errorf("number of hosts is wrong:expected:%d:got:%d:\n", 1, len(dest.Hosts))
	}
	if dest.Hosts[0].Id != "m3db_seed" {
		t.Errorf("hostname is wrong:expected:%s:got:%s:\n", "m3db_seed", dest.Hosts[0])
	}

}

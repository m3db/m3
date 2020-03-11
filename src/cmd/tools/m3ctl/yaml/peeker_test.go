package yaml

import (
	pb "github.com/m3db/m3/src/cmd/tools/m3ctl/yaml/generated"
	"io/ioutil"
	"testing"

	"github.com/gogo/protobuf/jsonpb"
)

func TestPeekerPositive(t *testing.T) {
	content, err := ioutil.ReadFile("./testdata/basicCreate.yaml")
	if err != nil {
		t.Fatalf("failed to read yaml test data:%v:\n", err)
	}
	urlpath, pbmessage, err := peeker(content)
	if err != nil {
		t.Fatalf("operation selector failed to encode the unknown operation yaml test data:%v:\n", err)
	}
	if urlpath != dbcreatePath {
		t.Errorf("urlpath is wrong:expected:%s:got:%s:\n", dbcreatePath, urlpath)
	}
	data, err := _load(content, pbmessage)
	if err != nil {
		t.Fatalf("failed to encode to protocol:%v:\n", err)
	}
	dest := pb.DatabaseCreateRequestYaml{}
	unmarshaller := &jsonpb.Unmarshaler{AllowUnknownFields: true}
	if err := unmarshaller.Unmarshal(data, &dest); err != nil {
		t.Fatalf("operation selector failed to unmarshal unknown operation data:%v:\n", err)
	}
	t.Logf("dest:%v:\n", dest)
	if dest.Request.NamespaceName != "default" {
		t.Errorf("dest NamespaceName does not have the correct value via operation:expected:%v:got:%v:", opCreate, dest.Request.NamespaceName)
	}
	if dest.Request.Type != "cluster" {
		t.Errorf("dest type does not have the correct value via operation:expected:%v:got:%v:", opCreate, dest.Request.Type)
	}
}
func TestPeekerNegative(t *testing.T) {
	content, err := ioutil.ReadFile("./testdata/unknownOperation.yaml")
	if err != nil {
		t.Fatalf("failed to read yaml test data:%v:\n", err)
	}
	if _, _, err := peeker(content); err == nil {
		t.Fatalf("operation selector should have returned an error\n")
	}
}

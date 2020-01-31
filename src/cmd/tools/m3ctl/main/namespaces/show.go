package namespaces

import (
	"fmt"
	"io"
	"log"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
	"github.com/m3db/m3/src/query/generated/proto/admin"

	"github.com/gogo/protobuf/jsonpb"
)

func Show(flags *NamespaceArgs, endpoint string) {
	url := fmt.Sprintf("%s%s?%s", endpoint, defaultPath, debugQS)
	if *flags.showAll {
		client.DoGet(url, client.Dumper)
	} else {
		client.DoGet(url, showNames)
	}
}

func showNames(in io.Reader) {
	registry := admin.NamespaceGetResponse{}
	unmarshaller := &jsonpb.Unmarshaler{AllowUnknownFields: true}
	if err := unmarshaller.Unmarshal(in, &registry); err != nil {
		log.Fatal(err)
	}
	for k, _ := range registry.Registry.Namespaces {
		fmt.Println(k)
	}
}

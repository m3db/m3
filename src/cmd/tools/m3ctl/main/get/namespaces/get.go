package namespaces

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/checkArgs"
	"io"
	"log"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
	common "github.com/m3db/m3/src/cmd/tools/m3ctl/main/namespaces"
	"github.com/m3db/m3/src/query/generated/proto/admin"

	"github.com/gogo/protobuf/jsonpb"
)

func doGet(flags *namespacesArgs, globals checkArgs.GlobalOpts) {
	url := fmt.Sprintf("%s%s?%s", globals.Endpoint, common.DefaultPath, common.DebugQS)
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

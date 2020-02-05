package namespaces

import (
	"fmt"
	"go.uber.org/zap"
	"io"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/globalopts"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/namespaces"
	"github.com/m3db/m3/src/query/generated/proto/admin"

	"github.com/gogo/protobuf/jsonpb"
)

func doGet(flags *namespacesVals, globals globalopts.GlobalOpts) error {
	url := fmt.Sprintf("%s%s?%s", globals.Endpoint, namespaces.DefaultPath, namespaces.DebugQS)
	if *flags.showAll {
		return client.DoGet(url, client.Dumper, globals.Zap)
	} else {
		return client.DoGet(url, showNames, globals.Zap)
	}
}

func showNames(in io.Reader, zl *zap.SugaredLogger) error {
	registry := admin.NamespaceGetResponse{}
	unmarshaller := &jsonpb.Unmarshaler{AllowUnknownFields: true}
	if err := unmarshaller.Unmarshal(in, &registry); err != nil {
		return err
	}
	for k, _ := range registry.Registry.Namespaces {
		fmt.Println(k)
	}
	return nil
}

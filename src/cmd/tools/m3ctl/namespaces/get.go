package namespaces

import (
	"fmt"
	"go.uber.org/zap"
	"io"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/client"
	"github.com/m3db/m3/src/query/generated/proto/admin"

	"github.com/gogo/protobuf/jsonpb"
)

func DoGet(endpoint string, showAll bool, zapper *zap.Logger) error {
	url := fmt.Sprintf("%s%s?%s", endpoint, DefaultPath, DebugQS)
	if showAll {
		return client.DoGet(url, client.Dumper, zapper)
	} else {
		return client.DoGet(url, showNames, zapper)
	}
}

func showNames(in io.Reader, zl *zap.Logger) error {
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

package namespaces

import (
	"fmt"
	"go.uber.org/zap"
	"io"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/http"
	"github.com/m3db/m3/src/query/generated/proto/admin"
)

const (
	defaultPath = "/api/v1/namespace"
	debugQS     = "debug=true"
)

type NamespaceArgs struct {
	showAll *bool
	delete  *string
}

func showNames(in io.Reader, log *zap.SugaredLogger) {
	registry := admin.NamespaceGetResponse{}
	unmarshaller := &jsonpb.Unmarshaler{AllowUnknownFields: true}
	if err := unmarshaller.Unmarshal(in, &registry); err != nil {
		log.Fatal(err)
	}
	for k, _ := range registry.Registry.Namespaces {
		fmt.Println(k)
	}
}

func Command(flags *NamespaceArgs, endpoint string, log *zap.SugaredLogger) {
	log.Debugf("NamespaceArgs:%+v:\n", flags)
	url := fmt.Sprintf("%s%s?%s", endpoint, defaultPath, debugQS)
	if *flags.showAll {
		http.DoGet(url, log, http.DoDump)
	} else if len(*flags.delete) > 0 {
		url = fmt.Sprintf("%s%s/%s", endpoint, "/api/v1/services/m3db/namespace", *flags.delete)
		http.DoDelete(url, log, http.DoDump)
	} else {
		http.DoGet(url, log, showNames)
	}
	return
}

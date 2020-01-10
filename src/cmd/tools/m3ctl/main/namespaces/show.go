package namespaces

import (
	"fmt"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/http"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"go.uber.org/zap"
	"io"
)

func Show(flags *NamespaceArgs, endpoint string, log *zap.SugaredLogger) {
	url := fmt.Sprintf("%s%s?%s", endpoint, defaultPath, debugQS)
	if *flags.showAll {
		http.DoGet(url, log, http.DoDump)
	} else {
		http.DoGet(url, log, showNames)
	}
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

//func ShowNames(flags *NamespaceArgs, endpoint string, log *zap.SugaredLogger) {
//	url := fmt.Sprintf("%s%s?%s", endpoint, defaultPath, debugQS)
//	http.DoGet(url, log, showNames)
//}


package namespaces

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/http"
	"go.uber.org/zap"
)

func Delete(flags *NamespaceArgs, endpoint string, log *zap.SugaredLogger) {
	url := fmt.Sprintf("%s%s/%s", endpoint, "/api/v1/services/m3db/namespace", *flags.delete)
	http.DoDelete(url, log, http.DoDump)
}

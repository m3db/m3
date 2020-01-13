package namespaces

import (
	"fmt"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
)

func Delete(flags *NamespaceArgs, endpoint string) {
	url := fmt.Sprintf("%s%s/%s", endpoint, "/api/v1/services/m3db/namespace", *flags.delete)
	client.DoDelete(url, client.DoDump)
}

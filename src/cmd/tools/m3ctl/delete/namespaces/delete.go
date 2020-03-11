package namespaces

import (
	"fmt"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/client"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/globalopts"
)

func doDelete(flags *namespacesVals, globals globalopts.GlobalOpts) error {
	url := fmt.Sprintf("%s%s/%s", globals.Endpoint, "/api/v1/services/m3db/namespace", *flags.nodeName)
	return client.DoDelete(url, client.Dumper, globals.Zap)
}

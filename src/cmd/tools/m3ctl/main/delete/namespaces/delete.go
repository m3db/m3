package namespaces

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/checkArgs"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
)

func doDelete(flags *namespacesArgs, globals checkArgs.GlobalOpts) {
	url := fmt.Sprintf("%s%s/%s", globals.Endpoint, "/api/v1/services/m3db/namespace", *flags.nodeName)
	client.DoDelete(url, client.Dumper)
}

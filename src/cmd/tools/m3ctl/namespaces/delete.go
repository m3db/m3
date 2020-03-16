package namespaces

import (
	"fmt"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/client"
	//"github.com/m3db/m3/src/cmd/tools/m3ctl/globalopts"
	"go.uber.org/zap"
)

//func doDelete(flags *namespacesVals, globals globalopts.GlobalOpts) error {
func DoDelete(endpoint string, nodeName string, deleteEntire bool, zapper *zap.Logger) error {

	url := fmt.Sprintf("%s%s/%s", endpoint, "/api/v1/services/m3db/namespace", nodeName)
	return client.DoDelete(url, client.Dumper, zapper)
}

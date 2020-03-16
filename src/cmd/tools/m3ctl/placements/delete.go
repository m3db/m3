package placements

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/client"
	"go.uber.org/zap"
)

func DoDelete(endpoint string, nodeName string, deleteEntire bool, zapper *zap.Logger) error {
	if deleteEntire {
		url := fmt.Sprintf("%s%s", endpoint, DefaultPath)
		return client.DoDelete(url, client.Dumper, zapper)
	}
	url := fmt.Sprintf("%s%s/%s", endpoint, DefaultPath, nodeName)
	return client.DoDelete(url, client.Dumper, zapper)
}

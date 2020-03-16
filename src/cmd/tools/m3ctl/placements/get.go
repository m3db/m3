package placements

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/client"
	"go.uber.org/zap"
)

func DoGet(endpoint string, zapper *zap.Logger) error {
	url := fmt.Sprintf("%s%s", endpoint, DefaultPath)
	return client.DoGet(url, client.Dumper, zapper)
}

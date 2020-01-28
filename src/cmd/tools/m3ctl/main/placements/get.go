package placements

import (
	"fmt"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
)

func (s XPlacementFlags) xget(endpoint string) {
	url := fmt.Sprintf("%s%s", endpoint, defaultPath)
	client.DoGet(url, client.DoDump)
	return
}

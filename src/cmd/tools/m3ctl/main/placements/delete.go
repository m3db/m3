package placements

import (
	"fmt"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
)

func Delete(flags *PlacementArgs, endpoint string) {
	if *flags.deletePlacement {
		url := fmt.Sprintf("%s%s", endpoint, defaultPath)
		client.DoDelete(url, client.DoDump)
		return
	}
	url := fmt.Sprintf("%s%s/%s", endpoint, defaultPath, *flags.deleteNode)
	client.DoDelete(url, client.DoDump)
	return
}

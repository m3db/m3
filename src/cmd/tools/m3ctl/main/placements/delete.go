package placements

import (
	"fmt"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
)

func doDelete(s placementArgs, endpoint string) {
	if *s.deletePlacement {
		url := fmt.Sprintf("%s%s", endpoint, defaultPath)
		client.DoDelete(url, client.Dumper)
		return
	}
	url := fmt.Sprintf("%s%s/%s", endpoint, defaultPath, *s.deleteNode)
	client.DoDelete(url, client.Dumper)
	return
}

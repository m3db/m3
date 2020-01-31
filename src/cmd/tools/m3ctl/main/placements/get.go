package placements

import (
	"fmt"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
)

func doGet(s placementArgs, endpoint string) {
	url := fmt.Sprintf("%s%s", endpoint, defaultPath)
	client.DoGet(url, client.Dumper)
	return
}

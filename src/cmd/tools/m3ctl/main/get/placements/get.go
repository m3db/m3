package placements

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/checkArgs"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
	common "github.com/m3db/m3/src/cmd/tools/m3ctl/main/placements"
)

func doGet(s *placementArgs, globals checkArgs.GlobalOpts) {
	url := fmt.Sprintf("%s%s", globals.Endpoint, common.DefaultPath)
	client.DoGet(url, client.Dumper)
	return
}

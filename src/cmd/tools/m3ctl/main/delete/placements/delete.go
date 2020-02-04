package placements

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/checkArgs"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
	common "github.com/m3db/m3/src/cmd/tools/m3ctl/main/placements"
)

func doDelete(s *placementArgs, globals checkArgs.GlobalOpts) {
	if *s.deleteEntire {
		url := fmt.Sprintf("%s%s", globals.Endpoint, common.DefaultPath)
		client.DoDelete(url, client.Dumper)
		return
	}
	url := fmt.Sprintf("%s%s/%s", globals.Endpoint, common.DefaultPath, *s.nodeName)
	client.DoDelete(url, client.Dumper)
	return
}

package placements

import (
	"fmt"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/globalopts"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/placements"
)

func doDelete(s *placementVals, globals globalopts.GlobalOpts) {
	if *s.deleteEntire {
		url := fmt.Sprintf("%s%s", globals.Endpoint, placements.DefaultPath)
		client.DoDelete(url, client.Dumper, globals.Zap)
		return
	}
	url := fmt.Sprintf("%s%s/%s", globals.Endpoint, placements.DefaultPath, *s.nodeName)
	client.DoDelete(url, client.Dumper, globals.Zap)
	return
}

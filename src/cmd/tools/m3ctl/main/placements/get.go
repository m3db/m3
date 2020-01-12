package placements

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/http"
)

func Get(flags *PlacementArgs, endpoint string) {
	url := fmt.Sprintf("%s%s", endpoint, defaultPath)
	http.DoGet(url, http.DoDump)
	return
}

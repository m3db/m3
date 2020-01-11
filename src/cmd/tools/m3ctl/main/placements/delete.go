package placements

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/http"
	"go.uber.org/zap"
)

func Delete(flags *PlacementArgs, endpoint string, log *zap.SugaredLogger) {
	if *flags.deletePlacement {
		url := fmt.Sprintf("%s%s", endpoint, defaultPath)
		http.DoDelete(url, log, http.DoDump)
		return
	}
	url := fmt.Sprintf("%s%s/%s", endpoint, defaultPath, *flags.deleteNode)
	http.DoDelete(url, log, http.DoDump)
	return
}

package placements

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/http"
	"go.uber.org/zap"
)

func Get(flags *PlacementArgs, endpoint string, log *zap.SugaredLogger) {
	url := fmt.Sprintf("%s%s", endpoint, defaultPath)
	http.DoGet(url, log, http.DoDump)
	return
}

package placements

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/http"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/yaml"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"go.uber.org/zap"
)

func Replace(flags *PlacementArgs, endpoint string, log *zap.SugaredLogger) {
	data := yaml.Load(flags.replaceFlag.Value[0], &admin.PlacementReplaceRequest{}, log)
	url := fmt.Sprintf("%s%s%s", endpoint, defaultPath, "/replace")
	http.DoPost(url, data, log, http.DoDump)
	return
}

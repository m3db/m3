package placements

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/http"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/yaml"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"go.uber.org/zap"
)

func Add(flags *PlacementArgs, endpoint string, log *zap.SugaredLogger) {
	log.Debugf("PlacementArgs:%+v:\n", flags)
	data := yaml.Load(flags.newNodeFlag.Value[0], &admin.PlacementInitRequest{}, log)
	url := fmt.Sprintf("%s%s", endpoint, defaultPath)
	http.DoPost(url, data, log, http.DoDump)
	return
}

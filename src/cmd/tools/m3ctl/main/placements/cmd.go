package placements

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/http"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/yaml"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"go.uber.org/zap"
)

const (
	defaultPath = "/api/v1/services/m3db/placement"
)

func Command(flags *PlacementArgs, endpoint string, log *zap.SugaredLogger) {
	log.Debugf("PlacementArgs:%+v:\n", flags)
	if len(*flags.deleteNode) > 0 {
		url := fmt.Sprintf("%s%s/%s", endpoint, defaultPath, *flags.deleteNode)
		http.DoDelete(url, log, http.DoDump)
	} else if len(flags.newNodeFlag.Value) > 0 && len(flags.newNodeFlag.Value) > 0 {
		data := yaml.Load(flags.newNodeFlag.Value[0], &admin.PlacementInitRequest{}, log)
		url := fmt.Sprintf("%s%s", endpoint, defaultPath)
		http.DoPost(url, data, log, http.DoDump)
	} else if len(flags.initFlag.Value) > 0 && len(flags.initFlag.Value[0]) > 0 {
		data := yaml.Load(flags.initFlag.Value[0], &admin.PlacementInitRequest{}, log)
		url := fmt.Sprintf("%s%s%s", endpoint, defaultPath, "/init")
		http.DoPost(url, data, log, http.DoDump)
	} else if len(flags.replaceFlag.Value) > 0 && len(flags.replaceFlag.Value[0]) > 0 {
		data := yaml.Load(flags.replaceFlag.Value[0], &admin.PlacementReplaceRequest{}, log)
		url := fmt.Sprintf("%s%s%s", endpoint, defaultPath, "/replace")
		http.DoPost(url, data, log, http.DoDump)
	} else if *flags.deletePlacement {
		url := fmt.Sprintf("%s%s", endpoint, defaultPath)
		http.DoDelete(url, log, http.DoDump)
	} else {
		url := fmt.Sprintf("%s%s", endpoint, defaultPath)
		http.DoGet(url, log, http.DoDump)
	}
	return
}

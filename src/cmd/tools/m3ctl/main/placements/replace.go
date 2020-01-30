package placements

import (
	"fmt"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/yaml"
	"github.com/m3db/m3/src/query/generated/proto/admin"
)

func xreplace(s placementArgs, endpoint string) {
	data := yaml.Load(s.replaceFlag.Value[0], &admin.PlacementReplaceRequest{})
	url := fmt.Sprintf("%s%s%s", endpoint, defaultPath, "/replace")
	client.DoPost(url, data, client.DoDump)
	return
}

package placements

import (
	"fmt"
	"log"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/yaml"
	"github.com/m3db/m3/src/query/generated/proto/admin"
)

func Add(flags *PlacementArgs, endpoint string) {
	log.Printf("PlacementArgs:%+v:\n", flags)
	data := yaml.Load(flags.newNodeFlag.Value[0], &admin.PlacementInitRequest{})
	url := fmt.Sprintf("%s%s", endpoint, defaultPath)
	client.DoPost(url, data, client.DoDump)
	return
}

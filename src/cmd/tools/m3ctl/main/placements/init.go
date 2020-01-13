package placements

import (
	"fmt"
	"log"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/yaml"
	"github.com/m3db/m3/src/query/generated/proto/admin"
)

func Init(flags *PlacementArgs, endpoint string) {
	log.Printf("PlacementArgs:%+v:\n", flags)
	data := yaml.Load(flags.initFlag.Value[0], &admin.PlacementInitRequest{})
	url := fmt.Sprintf("%s%s%s", endpoint, defaultPath, "/init")
	client.DoPost(url, data, client.DoDump)
	return
}

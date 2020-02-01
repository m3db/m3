package placements

import (
	"fmt"
	"log"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/yaml"
	"github.com/m3db/m3/src/query/generated/proto/admin"
)

func doInit(s placementArgs, endpoint string) {
	log.Printf("placementArgs:%+v:\n", s)
	data := yaml.Load(s.initFlag.Value[0], &admin.PlacementInitRequest{})
	url := fmt.Sprintf("%s%s%s", endpoint, DefaultPath, "/init")
	client.DoPost(url, data, client.Dumper)
	return
}

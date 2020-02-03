package placements

import (
	"log"
)

func doInit(s placementArgs, endpoint string) {
	log.Printf("placementArgs:%+v:\n", s)
	//data := yaml.Load(s.initFlag.Value[0], &admin.PlacementInitRequest{})
	//url := fmt.Sprintf("%s%s%s", endpoint, DefaultPath, "/init")
	//client.DoPost(url, data, client.Dumper)
	return
}

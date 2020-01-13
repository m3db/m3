package database

import (
	"fmt"
	"log"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/yaml"
	"github.com/m3db/m3/src/query/generated/proto/admin"
)

func Create(createYAML string, endpoint string) {
	log.Printf("createYAML:%s:\n", createYAML)
	data := yaml.Load(createYAML, &admin.DatabaseCreateRequest{})
	url := fmt.Sprintf("%s%s/create", endpoint, defaultPath)
	client.DoPost(url, data, client.DoDump)
	return
}

package database

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/http"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/yaml"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"log"
)

func Create(createYAML string, endpoint string) {
	log.Printf("createYAML:%s:\n", createYAML)
	data := yaml.Load(createYAML, &admin.DatabaseCreateRequest{})
	url := fmt.Sprintf("%s%s/create", endpoint, defaultPath)
	http.DoPost(url, data, http.DoDump)
	return
}

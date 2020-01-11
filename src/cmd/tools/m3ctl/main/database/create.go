package database

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/http"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/yaml"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"go.uber.org/zap"
)

const (
	defaultPath = "/api/v1/database"
)

func Create(createYAML string, endpoint string, log *zap.SugaredLogger) {
	log.Debugf("createYAML:%s:\n", createYAML)
	data := yaml.Load(createYAML, &admin.DatabaseCreateRequest{}, log)
	url := fmt.Sprintf("%s%s/create", endpoint, defaultPath)
	http.DoPost(url, data, log, http.DoDump)
	return
}

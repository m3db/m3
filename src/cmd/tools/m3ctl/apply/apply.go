package apply

import (
	"fmt"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/client"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/yaml"
)

func DoApply(endpoint string, filepath string, zapper *zap.Logger) error {
	//path, data, err := yaml.Load(vals.yamlFlag.Value[0], globals.Zap)
	path, data, err := yaml.Load(filepath, zapper)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s%s", endpoint, path)
	return client.DoPost(url, data, client.Dumper, zapper)
}

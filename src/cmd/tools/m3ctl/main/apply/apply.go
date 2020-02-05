package apply

import (
	"fmt"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/globalopts"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/yaml"
)

func doApply(vals *applyVals, globals globalopts.GlobalOpts) error {
	path, data, err := yaml.Load(vals.yamlFlag.Value[0], globals.Zap)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s%s", globals.Endpoint, path)
	return client.DoPost(url, data, client.Dumper, globals.Zap)
}

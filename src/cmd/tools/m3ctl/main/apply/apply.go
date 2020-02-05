package apply

import (
	"fmt"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/globalopts"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/yaml"
)

func doApply(vals *applyVals, globals globalopts.GlobalOpts) {
	path, data := yaml.Load(vals.yamlFlag.Value[0], globals.Zap)
	url := fmt.Sprintf("%s%s", globals.Endpoint, path)
	client.DoPost(url, data, client.Dumper, globals.Zap)
	return
}

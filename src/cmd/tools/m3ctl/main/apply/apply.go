package apply

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/checkArgs"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/yaml"
)

func Apply(vals *applyArgs, globals checkArgs.GlobalOpts) {
	path, data := yaml.Load(vals.yamlFlag.Value[0])
	url := fmt.Sprintf("%s%s", globals.Endpoint, path)
	client.DoPost(url, data, client.Dumper)
	return
}

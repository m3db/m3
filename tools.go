// +build tools

package tools

import (
	_ "github.com/fossas/fossa-cli/cmd/fossa"
	_ "github.com/garethr/kubeval"
	_ "github.com/golang/mock/mockgen"
	_ "github.com/golangci/golangci-lint/cmd/golangci-lint"
	_ "github.com/google/go-jsonnet/cmd/jsonnet"
	_ "github.com/m3db/build-tools/utilities/genclean"
	_ "github.com/m3db/tools/update-license"
	_ "github.com/mauricelam/genny"
	_ "github.com/mjibson/esc"
	_ "github.com/pointlander/peg"
	_ "github.com/rakyll/statik"
	_ "github.com/wjdp/htmltest"
)

// +build tools

import (
	_ "github.com/fossas/fossa-cli/cmd/fossa"
	_ "github.com/golang/mock/mockgen"
	_ "github.com/google/go-jsonnet/cmd/jsonnet"
	_ "github.com/m3db/build-tools/utilities/genclean"
	_ "github.com/m3db/tools/update-license"
	_ "github.com/Masterminds/glide"
	_ "github.com/mauricelam/genny"
	_ "github.com/mjibson/esc"
	_ "github.com/pointlander/peg"
	_ "github.com/rakyll/statik"
)

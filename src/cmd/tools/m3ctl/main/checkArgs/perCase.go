package checkArgs

import (
	"flag"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"
)

func CheckPerCase(args []string, fs *flag.FlagSet) *errors.FlagsError {
	if err := fs.Parse(args); err != nil {
		fs.Usage()
		return &errors.FlagsError{}
	}
	if fs.NFlag() == 0 {
		fs.Usage()
		return &errors.FlagsError{}
	}

	return nil
}


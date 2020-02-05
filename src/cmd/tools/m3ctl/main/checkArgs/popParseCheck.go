package checkArgs

import (
	"flag"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"
)

func PopParseAndCheck(args []string, fs *flag.FlagSet) error {
	thisArgs := args[1:]
	if err := fs.Parse(thisArgs); err != nil {
		fs.Usage()
		return err
	}
	if fs.NFlag() == 0 {
		fs.Usage()
		return &errors.FlagsError{}
	}
	return nil
}

package placements

import (
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/checkArgs"
	"testing"
)

func makeStub() Context {
	ctx := _setupFlags(
		&placementArgs{},
		placementHandlers{
			handle: func(*placementArgs, checkArgs.GlobalOpts) { return },
		},
	)
	ctx.Globals.Endpoint = "nuch"
	return ctx
}
func TestBasic(t *testing.T) {
	testData := []struct {
		args             []string
		msg              string
		successCondition func(error) bool
	}{
		{
			args:             []string{"pl", "-all"},
			msg:              "It should return no error for sane args for -all",
			successCondition: func(err error) bool { return err == nil },
		},
		{
			args:             []string{"pl", "-node", "eee"},
			msg:              "It should return no error for sane args for -node",
			successCondition: func(err error) bool { return err == nil },
		},
		{
			args:             []string{},
			msg:              "It should return error because we got here without pl",
			successCondition: func(err error) bool { return err != nil },
		},
		{
			args:             []string{"pl"},
			msg:              "It should return error because we got no args",
			successCondition: func(err error) bool { return err != nil },
		},
		{
			args:             []string{"pl", "-h"},
			msg:              "It should return error because we ran with -h",
			successCondition: func(err error) bool { return err != nil },
		},
		{
			args:             []string{"pl", "-node"},
			msg:              "It should return error because we ran with -node but no args",
			successCondition: func(err error) bool { return err != nil },
		},
		{
			args:             []string{"pl", "-node", "eee", "errr"},
			msg:              "It should return error because we got extra args",
			successCondition: func(err error) bool { return err != nil },
		},
		{
			args:             []string{"pl", "-node", ""},
			msg:              "It should return an error because we got an empty val",
			successCondition: func(err error) bool { return err == nil },
		},
	}
	for _, v := range testData {
		ctx := makeStub()
		rv := ctx.PopParseDispatch(v.args)
		if !v.successCondition(rv) {
			t.Error(v.msg)
		}
	}
}

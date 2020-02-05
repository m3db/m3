package namespaces

import (
	"testing"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/globalopts"
)

func makeStub() Context {
	ctx := _setupFlags(
		&namespacesVals{},
		namespacesHandlers{
			handle: func(*namespacesVals, globalopts.GlobalOpts) { return },
		},
	)
	ctx.Globals.Endpoint = "fakeEndpoint"
	return ctx
}
func TestBasic(t *testing.T) {
	testData := []struct {
		args             []string
		msg              string
		successCondition func(error) bool
	}{
		{
			args:             []string{"ns", "-node", "eee"},
			msg:              "It should return no error for sane args",
			successCondition: func(err error) bool { return err == nil },
		},
		{
			args:             []string{},
			msg:              "It should return error because we got here without ns",
			successCondition: func(err error) bool { return err != nil },
		},
		{
			args:             []string{"ns"},
			msg:              "It should return error because we got no args",
			successCondition: func(err error) bool { return err != nil },
		},
		{
			args:             []string{"ns", "-h"},
			msg:              "It should return error because we ran with -h",
			successCondition: func(err error) bool { return err != nil },
		},
		{
			args:             []string{"ns", "-node"},
			msg:              "It should return error because we ran with -node but no args",
			successCondition: func(err error) bool { return err != nil },
		},
		{
			args:             []string{"ns", "-node", "eee", "errr"},
			msg:              "It should return error because we got extra args",
			successCondition: func(err error) bool { return err != nil },
		},
		{
			args:             []string{"ns", "-node", ""},
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

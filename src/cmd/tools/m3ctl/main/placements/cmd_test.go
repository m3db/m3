package placements

import (
	"testing"
)

func stub1(placementArgs, string) { return }

func makeStub() Context {
	return _setupFlags(
		&placementArgs{},
		placementHandler{
			add:     stub1,
			delete:  stub1,
			xget:    stub1,
			xinit:   stub1,
			replace: stub1,
		},
	)
}
func TestBasic(t *testing.T) {

	testData := []struct {
		args []string
		ep string
		msg string
		successCondition func(error) bool
	}{
		{
			// no args not even pl
			args: []string{},
			ep: "",
			msg: "It should return error because we got here without pl",
			successCondition: func(err error) bool {return err != nil},
		},
		{
			// no args except pl
			args: []string{"pl"},
			ep: "",
			msg: "It should not return error no args",
			successCondition: func(err error) bool {return err == nil},
		},
		{
			args: []string{"pl", "junk"},
			ep: "",
			msg: "It should return error when given junk",
			successCondition: func(err error) bool {return err != nil},
		},
		{
			args: []string{"pl", "delete", "-all"},
			ep: "",
			msg: "It should not return error on sane delete all args",
			successCondition: func(err error) bool {return err == nil},
		},
		{
			args: []string{"pl", "delete", "-node", "nodeName"},
			ep: "",
			msg: "It should not return error on sane delete args",
			successCondition: func(err error) bool {return err == nil},
		},
		{
			args: []string{"pl", "add", "-f", "someFile"},
			ep: "",
			msg: "It should not return error on sane add args",
			successCondition: func(err error) bool {return err == nil},
		},
		{
			args: []string{"pl", "init", "-f", "someFile"},
			ep: "",
			msg: "It should not return error on sane init args",
			successCondition: func(err error) bool {return err == nil},
		},
		{
			args: []string{"pl", "replace", "-f", "someFile"},
			ep: "",
			msg: "It should not return error on sane replace args",
			successCondition: func(err error) bool {return err == nil},
		},
	}

	for _, v := range testData {
		ctx := makeStub()
		if ! v.successCondition(ctx.ParseAndDo(v.args, v.ep))  {
			t.Error(v.msg)
		}
	}

}

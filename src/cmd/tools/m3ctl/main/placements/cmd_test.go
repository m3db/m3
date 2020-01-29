package placements

import (
	"testing"
)

func stub1(s PlacementArgs, endpoint string) { return }

func makeStub() XPlacementFlags {
	return _setupFlags(
		&PlacementArgs{},
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

	// default is to list placements
	// so no args is OK
	flags := makeStub()
	if err := dispatcher([]string{}, flags, "http://localhost:13013"); err != nil {
		t.Error("It should not return error no args")
	}

	flags = makeStub()
	if err := dispatcher([]string{"junk"}, flags, ""); err == nil {
		t.Error("It should return an error on junk")
	}

	flags = makeStub()
	if err := dispatcher([]string{"delete", "-all"},flags, ""); err != nil {
		t.Error("It should not return error no sane args")
	}

	flags = makeStub()
	if err := dispatcher([]string{"delete", "-node", "nodeName"}, flags, ""); err != nil {
		t.Error("It should not return error no sane args")
	}

	flags = makeStub()
	if err := dispatcher([]string{"add", "-f", "somefile"}, flags, ""); err != nil {
		t.Error("It should not return error no sane args")
	}

	flags = makeStub()
	if err := dispatcher([]string{"init", "-f", "somefile"}, flags, ""); err != nil {
		t.Error("It should not return error no sane args")
	}

	flags = makeStub()
	if err := dispatcher([]string{"replace", "-f", "somefile"}, flags, ""); err != nil {
		t.Error("It should not return error no sane args")
	}

}

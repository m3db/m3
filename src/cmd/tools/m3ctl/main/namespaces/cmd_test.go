package namespaces

import (
	"testing"
)

func makeStub() (NamespaceArgs, NamespaceFlags) {
	namespaceArgs := NamespaceArgs{}
	namespaceFlagSets := SetupFlags(&namespaceArgs)

	namespaceFlagSets.NamespaceDoer = func(*NamespaceArgs, string) { return }
	namespaceFlagSets.DeleteDoer = func(*NamespaceArgs, string) { return }

	return namespaceArgs, namespaceFlagSets
}
func TestBasic(t *testing.T) {

	args, flags := makeStub()
	if e := parseAndDo([]string{""}, &args, &flags, ""); e != nil {
		t.Error("It should not return an error on no args")
	}

	args, flags = makeStub()
	if e := parseAndDo([]string{"delete", "-name", "www"}, &args, &flags, ""); e != nil {
		t.Error("It should not return error no sane args")
	}

	args, flags = makeStub()
	if e := parseAndDo([]string{"-all"}, &args, &flags, ""); e != nil {
		t.Error("It should not return error no sane args")
	}
}

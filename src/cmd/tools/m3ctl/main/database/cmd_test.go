package database

import (
	"github.com/m3db/m3/src/x/config/configflag"
	"testing"
)

func TestBasic(t *testing.T) {

	createDatabaseYAML := configflag.FlagStringSlice{}
	databaseFlagSets := SetupFlags(&createDatabaseYAML)

	if e := parseAndDoCreate([]string{}, &createDatabaseYAML, &databaseFlagSets, "", func(string, string) { return }); e == nil {
		t.Error("It should return error on no args")
	}

	createDatabaseYAML = configflag.FlagStringSlice{}
	databaseFlagSets = SetupFlags(&createDatabaseYAML)

	if e := parseAndDoCreate([]string{"create", "-f", "somefile"}, &createDatabaseYAML, &databaseFlagSets, "", func(string, string) { return }); e != nil {
		t.Error("It should NOT return error on sane args")
	}

	if len(createDatabaseYAML.Value) != 1 {
		t.Fatalf("db create filename len is wrong:expected:1:but got:%d:\n", len(createDatabaseYAML.Value))
	}

	if createDatabaseYAML.Value[0] != "somefile" {
		t.Errorf("db create filename value is wrong:expected:%s:but got:%s:\n", createDatabaseYAML.Value[0], "somefile")
	}

}

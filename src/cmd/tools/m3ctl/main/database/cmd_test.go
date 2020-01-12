package database

import (
	"github.com/m3db/m3/src/x/config/configflag"
	"testing"
)

func TestBasic(t *testing.T) {

	createDatabaseYAML := configflag.FlagStringSlice{}
	databaseFlagSets := SetupFlags(&createDatabaseYAML)

	if e := parseAndDoCreate([]string{""}, &createDatabaseYAML, &databaseFlagSets, "", func(string, string) { return }); e == nil {
		t.Error("It should return error on no args")
	}

	createDatabaseYAML = configflag.FlagStringSlice{}
	databaseFlagSets = SetupFlags(&createDatabaseYAML)

	if e := parseAndDoCreate([]string{"create", "-f", "somefile"}, &createDatabaseYAML, &databaseFlagSets, "", func(string, string) { return }); e != nil {
		t.Error("It should NOT return error on sane args")
	}
}

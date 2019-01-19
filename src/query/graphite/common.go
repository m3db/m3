package graphite

import (
	"fmt"
	"time"

	"github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/storage"
	"github.com/stretchr/testify/assert"
)

// QueryEngine is the generic engine interface.
type QueryEngine interface {
	FetchByQuery(
		ctx context.Context,
		query string,
		start, end time.Time,
		localOnly, useCache, useM3DB bool,
		timeout time.Duration,
	) (*storage.FetchResult, error)
}

// Equalish asserts that two objects are equal. Looser than assert.Equal since
// it checks for equality of printing expected vs printing actual.
//
//    assert.Equal(t, 123, 123, "123 and 123 should be equal")
//
// Returns whether the assertion was successful (true) or not (false).
func Equalish(
	t assert.TestingT,
	expected, actual interface{},
	msgAndArgs ...interface{},
) bool {
	if assert.ObjectsAreEqual(expected, actual) {
		return true
	}

	// Last ditch effort
	if fmt.Sprintf("%#v", expected) == fmt.Sprintf("%#v", actual) {
		return true
	}

	return assert.Fail(t, fmt.Sprintf("Not equal: %#v (expected)\n"+
		"        != %#v (actual)", expected, actual), msgAndArgs...)
}

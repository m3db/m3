package time

import (
	"fmt"
	"time"

	"github.com/golang/mock/gomock"
)

// Matcher is a gomock.Matcher that matches time.Time
type Matcher interface {
	gomock.Matcher
}

// NewMatcher returns a new Matcher
func NewMatcher(t time.Time) Matcher {
	return &matcher{t: t}
}

type matcher struct {
	t time.Time
}

func (m *matcher) Matches(x interface{}) bool {
	timeStruct, ok := x.(time.Time)
	if !ok {
		return false
	}
	return m.t.Equal(timeStruct)
}

func (m *matcher) String() string {
	return fmt.Sprintf("time: %s", m.t.String())
}

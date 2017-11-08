package time

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMatcher(t *testing.T) {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	t1 := time.Now().UTC()
	t2 := t1.In(loc)

	// Make sure t1 and t2 don't == eachother
	require.NotEqual(t, t1, t2)

	// Make sure t1 and t2 Match eachother
	t1Matcher := NewMatcher(t1)
	require.True(t, t1Matcher.Matches(t2))

	// Make sure the matcher doesn't always return true
	require.NotEqual(t, t1Matcher, t2.Add(1*time.Hour))
	require.NotEqual(t, t1Matcher, 10)
}

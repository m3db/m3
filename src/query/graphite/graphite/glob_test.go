package graphite

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlobToRegexPattern(t *testing.T) {
	tests := []struct {
		glob  string
		regex string
	}{
		{"cities\\+all.'china<1001>'.demarsk", "cities\\+all\\.+\\'china\\<1001\\>\\'\\.+demarsk"},
		{"foo.host.me{1,2,3}.*", "foo\\.+host\\.+me(1|2|3)\\.+[^\\.]*"},
		{"bar.zed.whatever[0-9].*.*.sjc1", "bar\\.+zed\\.+whatever[0-9]\\.+[^\\.]*\\.+[^\\.]*\\.+sjc1"},
		{"optic{0[3-9],1[0-9],20}", "optic(0[3-9]|1[0-9]|20)"},
	}

	for _, test := range tests {
		pattern, err := GlobToRegexPattern(test.glob)
		require.NoError(t, err)
		assert.Equal(t, test.regex, pattern, "bad pattern for %s", test.glob)
	}
}

func TestGlobToRegexPatternErrors(t *testing.T) {
	tests := []struct {
		glob string
		err  string
	}{
		{"foo.host{1,2", "unbalanced '{' in foo.host{1,2"},
		{"foo.host{1,2]", "invalid ']' at 12, no prior for '[' in foo.host{1,2]"},
		{"foo.,", "invalid ',' outside of matching group at pos 4 in foo.,"},
		{"foo.host{a[0-}", "invalid '}' at 13, no prior for '{' in foo.host{a[0-}"},
	}

	for _, test := range tests {
		_, err := GlobToRegexPattern(test.glob)
		require.Error(t, err)
		assert.Equal(t, test.err, err.Error(), "invalid error for %s", test.glob)
	}
}

func TestCompileGlob(t *testing.T) {
	tests := []struct {
		glob    string
		match   bool
		toMatch []string
	}{
		{"stats.sjc1.timers.rt-geofence.geofence??-sjc1.handler.query.count", true,
			[]string{
				"stats.sjc1.timers.rt-geofence.geofence01-sjc1.handler.query.count",
				"stats.sjc1.timers.rt-geofence.geofence24-sjc1.handler.query.count"}},
		{"stats.sjc1.timers.rt-geofence.geofence??-sjc1.handler.query.count", false,
			[]string{
				"stats.sjc1.timers.rt-geofence.geofence-sjc1.handler.query.count",
				"stats.sjc1.timers.rt-geofence.geofence.0-sjc1.handler.query.count",
				"stats.sjc1.timers.rt-geofence.geofence021-sjc1.handler.query.count",
				"stats.sjc1.timers.rt-geofence.geofence991-sjc1.handler.query.count"}},
		{"foo.host{1,2}.*", true,
			[]string{"foo.host1.zed", "foo.host2.whatever"}},
		{"foo.*.zed.*", true,
			[]string{"foo.bar.zed.eq", "foo.zed.zed.zed"}},
		{"foo.*.zed.*", false,
			[]string{"bar.bar.zed.zed", "foo.bar.zed", "foo.bar.zed.eq.monk"}},
		{"foo.host{1,2}.zed", true,
			[]string{"foo.host1.zed", "foo.host2.zed"}},
		{"foo.host{1,2}.zed", false,
			[]string{"foo.host3.zed", "foo.hostA.zed", "blad.host1.zed", "foo.host1.zed.z"}},
		{"optic{0[3-9],1[0-9],20}", true,
			[]string{"optic03", "optic10", "optic20"}},
		{"optic{0[3-9],1[0-9],20}", false,
			[]string{"optic01", "optic21", "optic201", "optic031"}},
	}

	for _, test := range tests {
		rePattern, err := GlobToRegexPattern(test.glob)
		require.NoError(t, err)
		re := regexp.MustCompile(fmt.Sprintf("^%s$", rePattern))
		for _, s := range test.toMatch {
			matched := re.MatchString(s)
			assert.Equal(t, test.match, matched, "incorrect match between %s and %s", test.glob, s)
		}
	}
}

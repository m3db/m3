package tagfiltertree

import (
	"bufio"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	_tagFilters = `
	service:foo1 tagName1:tagValue1 tagName2:tagValue2
	service:foo2 tagName3:tagValue3 tagName4:tagValue4
	service:foo3 tagName5:tagValue5 tagName6:tagValue6
	service:foo4 tagName7:tagValue7 tagName8:tagValue8
	service:foo5 tagName9:tagValue9 tagName10:tagValue10
	service:foo6 tagName11:tagValue11 tagName12:tagValue12
	service:foo7 tagName13:tagValue13 tagName14:tagValue14
	service:foo8 tagName15:tagValue15 tagName16:tagValue16
	service:foo9 tagName17:tagValue17 tagName18:tagValue18
	service:foo10 tagName19:tagValue19 tagName20:tagValue20
	service:foo11 tagName1:tagValue21 tagName2:tagValue22
	service:foo12 tagName3:tagValue23 tagName4:tagValue24
	service:foo13 tagName5:tagValue25 tagName6:tagValue26
	service:foo14 tagName7:tagValue27 tagName8:tagValue28
	service:foo15 tagName9:tagValue29 tagName10:tagValue30
	service:foo16 tagName11:tagValue31 tagName12:tagValue32
	service:foo17 tagName13:tagValue33 tagName14:tagValue34
	service:foo18 tagName15:tagValue35 tagName16:tagValue36
	service:foo19 tagName17:tagValue37 tagName18:tagValue38
	service:foo20 tagName19:tagValue39 tagName20:tagValue40
	service:foo21 tagName1:tagValue41 tagName2:tagValue42
	service:foo22 tagName3:tagValue43 tagName4:tagValue44
	service:foo23 tagName5:tagValue45 tagName6:tagValue46
	service:foo24 tagName7:tagValue47 tagName8:tagValue48
	service:foo25 tagName9:tagValue49 tagName10:tagValue50
	service:foo26 tagName11:tagValue51 tagName12:tagValue52
	service:foo27 tagName13:tagValue53 tagName14:tagValue54
	service:foo28 tagName15:tagValue55 tagName16:tagValue56
	service:foo29 tagName17:tagValue57 tagName18:tagValue58
	service:foo30 tagName19:tagValue59 tagName20:tagValue60
	service:foo31 tagName1:tagValue61 tagName2:tagValue62
	service:foo32 tagName3:tagValue63 tagName4:tagValue64
	service:foo33 tagName5:tagValue65 tagName6:tagValue66
	service:foo34 tagName7:tagValue67 tagName8:tagValue68
	service:foo35 tagName9:tagValue69 tagName10:tagValue70
	service:foo36 tagName11:tagValue71 tagName12:tagValue72
	service:foo37 tagName13:tagValue73 tagName14:tagValue74
	service:foo38 tagName15:tagValue75 tagName16:tagValue76
	service:foo39 tagName17:tagValue77 tagName18:tagValue78
	service:foo40 tagName19:tagValue79 tagName20:tagValue80
	service:foo41 tagName1:tagValue81 tagName2:tagValue82
	service:foo42 tagName3:tagValue83 tagName4:tagValue84
	service:foo43 tagName5:tagValue85 tagName6:tagValue86
	service:foo44 tagName7:tagValue87 tagName8:tagValue88
	service:foo45 tagName9:tagValue89 tagName10:tagValue90
	service:foo46 tagName11:tagValue91 tagName12:tagValue92
	service:foo47 tagName13:tagValue93 tagName14:tagValue94
	service:foo48 tagName15:tagValue95 tagName16:tagValue96
	service:foo49 tagName17:tagValue97 tagName18:tagValue98
	service:foo50 tagName19:tagValue99 tagName20:tagValue100
	service:foo51 tagName1:tagValue101 tagName2:tagValue102
	service:foo52 tagName3:tagValue103 tagName4:tagValue104
	service:foo53 tagName5:tagValue105 tagName6:tagValue106
	service:foo54 tagName7:tagValue107 tagName8:tagValue108
	service:foo55 tagName9:tagValue109 tagName10:tagValue110
	service:foo56 tagName11:tagValue111 tagName12:tagValue112
	service:foo57 tagName13:tagValue113 tagName14:tagValue114
	service:foo58 tagName15:tagValue115 tagName16:tagValue116
	service:foo59 tagName17:tagValue117 tagName18:tagValue118
	service:foo60 tagName19:tagValue119 tagName20:tagValue120
	service:foo61 tagName1:tagValue121 tagName2:tagValue122
	service:foo62 tagName3:tagValue123 tagName4:tagValue124
	service:foo63 tagName5:tagValue125 tagName6:tagValue126
	service:foo64 tagName7:tagValue127 tagName8:tagValue128
	service:foo65 tagName9:tagValue129 tagName10:tagValue130
	service:foo66 tagName11:tagValue131 tagName12:tagValue132
	service:foo67 tagName13:tagValue133 tagName14:tagValue134
	service:foo68 tagName15:tagValue135 tagName16:tagValue136
	service:foo69 tagName17:tagValue137 tagName18:tagValue138
	service:foo70 tagName19:tagValue139 tagName20:tagValue140
	service:foo71 tagName1:tagValue141 tagName2:tagValue142
	service:foo72 tagName3:tagValue143 tagName4:tagValue144
	service:foo73 tagName5:tagValue145 tagName6:tagValue146
	service:foo74 tagName7:tagValue147 tagName8:tagValue148
	service:foo75 tagName9:tagValue149 tagName10:tagValue150
	service:foo76 tagName11:tagValue151 tagName12:tagValue152
	service:foo77 tagName13:tagValue153 tagName14:tagValue154
	service:foo78 tagName15:tagValue155 tagName16:tagValue156
	service:foo79 tagName17:tagValue157 tagName18:tagValue158
	service:foo80 tagName19:tagValue159 tagName20:tagValue160
	service:foo81 tagName1:tagValue161 tagName2:tagValue162
	service:foo82 tagName3:tagValue163 tagName4:tagValue164
	service:foo83 tagName5:tagValue165 tagName6:tagValue166
	service:foo84 tagName7:tagValue167 tagName8:tagValue168
	service:foo85 tagName9:tagValue169 tagName10:tagValue170
	service:foo86 tagName11:tagValue171 tagName12:tagValue172
	service:foo87 tagName13:tagValue173 tagName14:tagValue174
	service:foo88 tagName15:tagValue175 tagName16:tagValue176
	service:foo89 tagName17:tagValue177 tagName18:tagValue178
	service:foo90 tagName19:tagValue179 tagName20:tagValue180
	service:foo91 tagName1:tagValue181 tagName2:tagValue182
	service:foo92 tagName3:tagValue183 tagName4:tagValue184
	service:foo93 tagName5:tagValue185 tagName6:tagValue186
	service:foo94 tagName7:tagValue187 tagName8:tagValue188
	service:foo95 tagName9:tagValue189 tagName10:tagValue190
	service:foo96 tagName11:tagValue191 tagName12:tagValue192
	service:foo97 tagName13:tagValue193 tagName14:tagValue194
	service:foo98 tagName15:tagValue195 tagName16:tagValue196
	service:foo99 tagName17:tagValue197 tagName18:tagValue198
	service:foo100 tagName19:tagValue199 tagName20:tagValue200
`

	_inputTags = `
	service:foo100 tagName19:tagValue199 tagName20:tagValue200
`
	_namespaceLen = 10
)

func BenchmarkTreeGetData(b *testing.B) {
	// create a new tree
	tree := New[*Rule]()

	// add multiple tag filters to the tree
	for _, rule := range generateRules() {
		rule := rule
		for _, tf := range rule.TagFilters {
			tags, err := TagsFromTagFilter(tf)
			require.NoError(b, err)
			tree.AddTagFilter(tags, &rule)
		}
	}
}

func generateRules() []Rule {
	scanner := bufio.NewScanner(strings.NewReader(_tagFilters))
	rules := make([]Rule, 0)
	for scanner.Scan() {
		tf := scanner.Text()
		tfs := make([]string, 0)
		tfs = append(tfs, tf)
		rules = append(rules, Rule{
			TagFilters: tfs,
			Namespace:  generateRandomString(_namespaceLen),
		})
	}

	return rules
}

func generateRandomString(n int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[generateRandomInt(len(charset))]
	}
	return string(b)
}

func generateRandomInt(n int) int {
	// generate a random num between 0 and n-1.
	return rand.Intn(n)
}

// +build ignore

package gorules

import "github.com/quasilyte/go-ruleguard/dsl/fluent"

func _(m fluent.Matcher) {
	m.Match(`map[$k]$v`).Where(m["k"].Type.Is("time.Time")).Report(`time.Time used as map key`)
}

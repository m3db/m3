package native

import (
	"fmt"
	"math"
	"testing"

	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/ts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testCompile struct {
	input  string
	result interface{}
}

func hello(ctx *common.Context) (string, error)         { return "hello", nil }
func noArgs(ctx *common.Context) (ts.SeriesList, error) { return ts.SeriesList{}, nil }
func defaultArgs(ctx *common.Context, b bool, f1, f2 float64, s string) (ts.SeriesList, error) {
	return ts.SeriesList{}, nil
}

func TestCompile1(t *testing.T) {
	sortByName := findFunction("sortByName")
	noArgs := findFunction("noArgs")
	aliasByNode := findFunction("aliasByNode")
	summarize := findFunction("summarize")
	defaultArgs := findFunction("defaultArgs")
	sumSeries := findFunction("sumSeries")
	asPercent := findFunction("asPercent")
	scale := findFunction("scale")

	tests := []testCompile{
		{"", noopExpression{}},
		{"foo.bar.{a,b,c}.carbon-*.stat[0-9]",
			newFetchExpression("foo.bar.{a,b,c}.carbon-*.stat[0-9]")},
		{"noArgs()", &funcExpression{&functionCall{f: noArgs}}},
		{"sortByName(foo.bar.zed)", &funcExpression{
			&functionCall{
				f: sortByName,
				in: []funcArg{
					newFetchExpression("foo.bar.zed"),
				},
			},
		}},
		{"aliasByNode(statsdex.cluster4.*.metrics.written, 2, 4)", &funcExpression{
			&functionCall{
				f: aliasByNode,
				in: []funcArg{
					newFetchExpression("statsdex.cluster4.*.metrics.written"),
					newIntConst(2),
					newIntConst(4),
				},
			},
		}},
		{"summarize(foo.bar.baz.quux, \"1h\", \"max\", TRUE)", &funcExpression{
			&functionCall{
				f: summarize,
				in: []funcArg{
					newFetchExpression("foo.bar.baz.quux"),
					newStringConst("1h"),
					newStringConst("max"),
					newBoolConst(true),
				},
			},
		}},
		{"summarize(foo.bar.baz.quuz, \"1h\")", &funcExpression{
			&functionCall{
				f: summarize,
				in: []funcArg{
					newFetchExpression("foo.bar.baz.quuz"),
					newStringConst("1h"),
					newStringConst(""),
					newBoolConst(false),
				},
			},
		}},
		{"defaultArgs(true)", &funcExpression{
			&functionCall{
				f: defaultArgs,
				in: []funcArg{
					newBoolConst(true),          // non-default value
					newFloat64Const(math.NaN()), // default value
					newFloat64Const(100),        // default value
					newStringConst("foobar"),    // default value
				},
			},
		}},
		{"sortByName(aliasByNode(statsdex.cluster72.*.metrics.written,2,4,6))", &funcExpression{
			&functionCall{
				f: sortByName,
				in: []funcArg{
					&functionCall{
						f: aliasByNode,
						in: []funcArg{
							newFetchExpression("statsdex.cluster72.*.metrics.written"),
							newIntConst(2),
							newIntConst(4),
							newIntConst(6),
						},
					},
				},
			},
		}},
		{"sumSeries(foo.bar.baz.quux, statsdex.cluster72.*.metrics.written)", &funcExpression{
			&functionCall{
				f: sumSeries,
				in: []funcArg{
					newFetchExpression("foo.bar.baz.quux"),
					newFetchExpression("statsdex.cluster72.*.metrics.written"),
				},
			},
		}},
		{"asPercent(statsdex.cluster72.*.metrics.written, foo.bar.baz.quux)", &funcExpression{
			&functionCall{
				f: asPercent,
				in: []funcArg{
					newFetchExpression("statsdex.cluster72.*.metrics.written"),
					newFetchExpression("foo.bar.baz.quux"),
				},
			},
		}},
		{"asPercent(statsdex.cluster72.*.metrics.written, sumSeries(foo.bar.baz.quux))", &funcExpression{
			&functionCall{
				f: asPercent,
				in: []funcArg{
					newFetchExpression("statsdex.cluster72.*.metrics.written"),
					&functionCall{
						f: sumSeries,
						in: []funcArg{
							newFetchExpression("foo.bar.baz.quux"),
						},
					},
				},
			},
		}},
		{"asPercent(statsdex.cluster72.*.metrics.written, 100)", &funcExpression{
			&functionCall{
				f: asPercent,
				in: []funcArg{
					newFetchExpression("statsdex.cluster72.*.metrics.written"),
					newIntConst(100),
				},
			},
		}},
		{"asPercent(statsdex.cluster72.*.metrics.written)", &funcExpression{
			&functionCall{
				f: asPercent,
				in: []funcArg{
					newFetchExpression("statsdex.cluster72.*.metrics.written"),
					newConstArg([]*ts.Series(nil)),
				},
			},
		}},
		{"asPercent(statsdex.cluster72.*.metrics.written, total=sumSeries(foo.bar.baz.quux))", &funcExpression{
			&functionCall{
				f: asPercent,
				in: []funcArg{
					newFetchExpression("statsdex.cluster72.*.metrics.written"),
					&functionCall{
						f: sumSeries,
						in: []funcArg{
							newFetchExpression("foo.bar.baz.quux"),
						},
					},
				},
			},
		}},
		{"asPercent(statsdex.cluster72.*.metrics.written, total=100)", &funcExpression{
			&functionCall{
				f: asPercent,
				in: []funcArg{
					newFetchExpression("statsdex.cluster72.*.metrics.written"),
					newIntConst(100),
				},
			},
		}},
		{"scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, 1e+3)", &funcExpression{
			&functionCall{
				f: scale,
				in: []funcArg{
					newFetchExpression("servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*"),
					newFloat64Const(1000),
				},
			},
		}},
		{"scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, 1e-3)", &funcExpression{
			&functionCall{
				f: scale,
				in: []funcArg{
					newFetchExpression("servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*"),
					newFloat64Const(0.001),
				},
			},
		}},
		{"scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, 1e3)", &funcExpression{
			&functionCall{
				f: scale,
				in: []funcArg{
					newFetchExpression("servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*"),
					newFloat64Const(1000),
				},
			},
		}},
		{"scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, 1.1e3)", &funcExpression{
			&functionCall{
				f: scale,
				in: []funcArg{
					newFetchExpression("servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*"),
					newFloat64Const(1100),
				},
			},
		}},
		{"scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, 1.1e+3)", &funcExpression{
			&functionCall{
				f: scale,
				in: []funcArg{
					newFetchExpression("servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*"),
					newFloat64Const(1100),
				},
			},
		}},
		{"scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, 1.2e-3)", &funcExpression{
			&functionCall{
				f: scale,
				in: []funcArg{
					newFetchExpression("servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*"),
					newFloat64Const(0.0012),
				},
			},
		}},
		{"scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, .1e+3)", &funcExpression{
			&functionCall{
				f: scale,
				in: []funcArg{
					newFetchExpression("servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*"),
					newFloat64Const(100),
				},
			},
		}},
		{"scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, 2.e+3)", &funcExpression{
			&functionCall{
				f: scale,
				in: []funcArg{
					newFetchExpression("servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*"),
					newFloat64Const(2000),
				},
			},
		}},
	}

	for _, test := range tests {
		expr, err := compile(test.input)
		require.Nil(t, err, "error compiling: expression='%s', error='%v'", test.input, err)
		require.NotNil(t, expr)
		assertExprTree(t, test.result, expr, fmt.Sprintf("invalid result for %s: %v vs %v",
			test.input, test.result, expr))
	}
}

type testCompilerError struct {
	input string
	err   string
}

func TestCompileErrors(t *testing.T) {
	tests := []testCompilerError{
		{"hello()", "top-level functions must return timeseries data"},
		{"sortByName(foo.*.zed)junk", "invalid expression 'sortByName(foo.*.zed)junk': " +
			"extra data junk"},
		{"aliasByNode(",
			"invalid expression 'aliasByNode(': unexpected eof while parsing aliasByNode"},
		{"unknownFunc()",
			"invalid expression 'unknownFunc()': could not find function named unknownFunc"},
		{"aliasByNode(10)",
			"invalid expression 'aliasByNode(10)': invalid function call aliasByNode," +
				" arg 0: expected a singlePathSpec, received '10'"},
		{"sortByName(hello())",
			"invalid expression 'sortByName(hello())': invalid function call " +
				"sortByName, arg 0: expected a singlePathSpec, received 'hello()'"},
		{"aliasByNode()",
			"invalid expression 'aliasByNode()': invalid number of arguments for aliasByNode;" +
				" expected at least 2, received 0"},
		{"aliasByNode(foo.*.zed)", // check that at least 1 param is provided for variadic args
			"invalid expression 'aliasByNode(foo.*.zed)': invalid number of arguments for " +
				"aliasByNode; expected at least 2, received 1"},
		{"aliasByNode(foo.*.zed, 2, false)",
			"invalid expression 'aliasByNode(foo.*.zed, 2, false)': invalid function call " +
				"aliasByNode, arg 2: expected a int, received 'false'"},
		{"aliasByNode(foo.*.bar,",
			"invalid expression 'aliasByNode(foo.*.bar,': unexpected eof while" +
				" parsing aliasByNode"},
		{"aliasByNode(foo.*.bar,)",
			"invalid expression 'aliasByNode(foo.*.bar,)': invalid function call" +
				" aliasByNode, arg 1: invalid expression 'aliasByNode(foo.*.bar,)': ) not valid"},
		// TODO(jayp): Not providing all required parameters in a function with default parameters
		// leads to an error message that states that a greater than required number of expected
		// arguments. We could do better, but punting for now.
		{"summarize(foo.bar.baz.quux)",
			"invalid expression 'summarize(foo.bar.baz.quux)':" +
				" invalid number of arguments for summarize; expected 4, received 1"},
		{"sumSeries()", // check that at least 1 series is provided for variadic timeSeriesList
			"invalid expression 'sumSeries()': invalid number of arguments for sumSeries;" +
				" expected at least 1, received 0"},
		{"sumSeries(foo.bar.baz.quux,)",
			"invalid expression 'sumSeries(foo.bar.baz.quux,)': invalid function call sumSeries, " +
				"arg 1: invalid expression 'sumSeries(foo.bar.baz.quux,)': ) not valid"},
		{"asPercent(statsdex.cluster72.*.metrics.written, total",
			"invalid expression 'asPercent(statsdex.cluster72.*.metrics.written, total': " +
				"invalid function call asPercent, " +
				"arg 1: invalid expression 'asPercent(statsdex.cluster72.*.metrics.written, total': " +
				"unexpected eof, total should be followed by = or ("},
		{"asPercent(statsdex.cluster72.*.metrics.written, total=",
			"invalid expression 'asPercent(statsdex.cluster72.*.metrics.written, total=': " +
				"invalid function call asPercent, " +
				"arg 1: invalid expression 'asPercent(statsdex.cluster72.*.metrics.written, total=': " +
				"unexpected eof, named argument total should be followed by its value"},
		{"asPercent(statsdex.cluster72.*.metrics.written, total=randomStuff",
			"invalid expression 'asPercent(statsdex.cluster72.*.metrics.written, total=randomStuff': " +
				"invalid function call asPercent, " +
				"arg 1: invalid expression 'asPercent(statsdex.cluster72.*.metrics.written, total=randomStuff': " +
				"unexpected eof, randomStuff should be followed by = or ("},
		{"asPercent(statsdex.cluster72.*.metrics.written, total=sumSeries(",
			"invalid expression 'asPercent(statsdex.cluster72.*.metrics.written, total=sumSeries(': " +
				"invalid function call asPercent, " +
				"arg 1: invalid expression 'asPercent(statsdex.cluster72.*.metrics.written, total=sumSeries(': " +
				"unexpected eof while parsing sumSeries"},
		{"scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, 1.e)",
			"invalid expression 'scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, 1.e)': " +
				"invalid function call scale, " +
				"arg 1: invalid expression 'scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, 1.e)': " +
				"expected one of 0123456789, found ) not valid"},
		{"scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, .1e)",
			"invalid expression 'scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, .1e)': " +
				"invalid function call scale, " +
				"arg 1: invalid expression 'scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, .1e)': " +
				"expected one of 0123456789, found ) not valid"},
		{"scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, .e)",
			"invalid expression 'scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, .e)': " +
				"invalid function call scale, " +
				"arg 1: invalid expression 'scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, .e)': " +
				"expected one of 0123456789, found e not valid"},
		{"scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, e)",
			"invalid expression 'scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, e)': " +
				"invalid function call scale, " +
				"arg 1: invalid expression 'scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, e)': " +
				"could not find function named e"},
		{"scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, 1.2ee)",
			"invalid expression 'scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, 1.2ee)': " +
				"invalid function call scale, " +
				"arg 1: invalid expression 'scale(servers.appdocker*-sjc1.docker.usi-atmmachine-atmmachine.cpu.*, 1.2ee)': " +
				"expected one of 0123456789, found e not valid"},
	}

	for _, test := range tests {
		expr, err := compile(test.input)
		require.NotNil(t, err, "no error for %s", test.input)
		assert.Equal(t, test.err, err.Error(), "wrong error for %s", test.input)
		assert.Nil(t, expr, "non-nil expression for %s", test.input)
	}
}

func assertExprTree(t *testing.T, expected interface{}, actual interface{}, msg string) {
	switch e := expected.(type) {
	case *functionCall:
		a, ok := actual.(*functionCall)
		require.True(t, ok, msg)
		require.Equal(t, e.f.name, a.f.name, msg)
		require.Equal(t, len(e.f.in), len(a.f.in), msg)
		for i := range e.in {
			assertExprTree(t, e.in[i], a.in[i], msg)
		}
	case noopExpression:
		_, ok := actual.(noopExpression)
		require.True(t, ok, msg)
	case *funcExpression:
		a, ok := actual.(*funcExpression)
		require.True(t, ok, msg)
		assertExprTree(t, e.call, a.call, msg)
	case *fetchExpression:
		a, ok := actual.(*fetchExpression)
		require.True(t, ok, msg)
		assert.Equal(t, e.pathArg.path, a.pathArg.path, msg)
	case constFuncArg:
		a, ok := actual.(constFuncArg)
		require.True(t, ok, msg)
		assert.Equal(t, e.value.Interface(), a.value.Interface(), msg)
	default:
		assert.Equal(t, expected, actual, msg)
	}
}

func TestExtractFetchExpressions(t *testing.T) {
	tests := []struct {
		expr    string
		targets []string
	}{
		{"summarize(groupByNode(nonNegativeDerivative(stats.sjc1.gauges.apollo.production.neko.streamio_schemaless_mezzanine_trips_sjc1_streaming.*.client_trip_base.numMessagesReceived.count), 8, 'sum'), '10min', 'avg', true)", []string{
			"stats.sjc1.gauges.apollo.production.neko.streamio_schemaless_mezzanine_trips_sjc1_streaming.*.client_trip_base.numMessagesReceived.count",
		}},
		{"asPercent(statsdex.cluster72.*.metrics.written, total=sumSeries(foo.bar.baz.quux))", []string{
			"statsdex.cluster72.*.metrics.written", "foo.bar.baz.quux",
		}},
		{"foo.bar.{a,b,c}.carbon-*.stat[0-9]", []string{
			"foo.bar.{a,b,c}.carbon-*.stat[0-9]",
		}},
	}

	for _, test := range tests {
		targets, err := ExtractFetchExpressions(test.expr)
		require.NoError(t, err)
		assert.Equal(t, test.targets, targets, test.expr)
	}

}

func init() {
	MustRegisterFunction(noArgs)
	MustRegisterFunction(hello)
	MustRegisterFunction(defaultArgs).WithDefaultParams(map[uint8]interface{}{
		1: false,
		2: math.NaN(),
		3: 100,
		4: "foobar",
	})
}

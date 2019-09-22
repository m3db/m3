package integration

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/mock"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

func TestQuerySimplePropertyTest(t *testing.T) {
	binaryPropertyFunc := func(input propTestMultiInput) (bool, error) {
		query := binaryExpression{
			leftExpr:  queryExpression{query: "requests{}"},
			rightExpr: input.inputs[0].expression,
			op:        ">",
		}.String()

		fmt.Printf("exec query: %v\n", query)

		values, bounds := test.GenerateValuesAndBounds(nil, nil)
		b := test.NewBlockFromValues(bounds, values)
		mockStorage := mock.NewMockStorage()
		mockStorage.SetFetchBlocksResult(block.Result{Blocks: []block.Block{b}}, nil)

		var (
			ctx        = context.Background()
			now        = time.Now()
			engineOpts = executor.NewEngineOptions().
					SetStore(mockStorage).
					SetInstrumentOptions(instrument.NewOptions())
			engine      = executor.NewEngine(engineOpts)
			opts        = &executor.QueryOptions{}
			fetchOpts   = &storage.FetchOptions{}
			queryWindow = time.Hour
			queryStep   = 15 * time.Second
			params      = models.RequestParams{
				Start: now.Add(-1 * queryWindow),
				End:   now,
				Step:  queryStep,
				Query: query,
			}
		)
		parser, err := promql.Parse(query, models.NewTagOptions())
		require.NoError(t, err)

		result, err := engine.ExecuteExpr(ctx, parser, opts, fetchOpts, params)
		require.NoError(t, err)

		series, err := native.NewSeriesFromResult(result)
		require.NoError(t, err)

		// TODO: Better verification.
		require.True(t, len(series) > 0)
		return true, nil
	}

	parameters := gopter.DefaultTestParameters()
	parameters.Rng.Seed(123456789)
	parameters.MinSuccessfulTests = 10
	props := gopter.NewProperties(parameters)
	props.Property("Test binary operator",
		prop.ForAll(binaryPropertyFunc, genPropTestMultiInputs(propTestMultiInputsOptions{
			inputsOpts: []propTestInputsOptions{
				{
					exprNumMin: 2,
					exprNumMax: 5,
					exprType:   scalarExpressionType,
				},
			},
		})))

	props.TestingRun(t)
}

type expressionType uint

const (
	constantExpressionType expressionType = iota
	scalarExpressionType
	binaryExpressionType
	queryExpressionType
)

type propTestInput struct {
	expression testExpression
}

type propTestMultiInput struct {
	inputs []propTestInput
}

type propTestMultiInputsOptions struct {
	inputsOpts []propTestInputsOptions
}

func genPropTestMultiInputs(opts propTestMultiInputsOptions) gopter.Gen {
	var gens []gopter.Gen
	for _, subOpts := range opts.inputsOpts {
		gens = append(gens, genPropTestInputs(subOpts))
	}
	return gopter.CombineGens(gens...).FlatMap(func(input interface{}) gopter.Gen {
		inputs := input.([]interface{})

		var converted []propTestInput
		for _, input := range inputs {
			converted = append(converted, input.(propTestInput))
		}

		return gopter.CombineGens(gen.Bool()).
			Map(func(input interface{}) propTestMultiInput {
				return propTestMultiInput{inputs: converted}
			})
	}, reflect.TypeOf(propTestMultiInput{}))
}

type propTestInputsOptions struct {
	exprNumMin int
	exprNumMax int
	exprType   expressionType
}

func genPropTestInputs(opts propTestInputsOptions) gopter.Gen {
	return gopter.CombineGens(
		gen.IntRange(opts.exprNumMin, opts.exprNumMax),
	).FlatMap(func(input interface{}) gopter.Gen {
		inputs := input.([]interface{})
		numExpressions := inputs[0].(int)
		return genPropTestInput(numExpressions, opts.exprType)
	}, reflect.TypeOf(propTestInput{}))
}

func genPropTestInput(numExpressions int, exprType expressionType) gopter.Gen {
	return gopter.CombineGens(
		genExpression(genExpressionOptions{
			exprNum:  numExpressions,
			exprType: exprType,
		}),
	).Map(func(inputs []interface{}) propTestInput {
		return propTestInput{
			expression: inputs[0].(testExpression),
		}
	})
}

type genExpressionOptions struct {
	exprNum  int
	exprType expressionType
}

type testExpression interface {
	String() string
}

type constantExpression struct {
	value float64
}

func (e constantExpression) String() string {
	return fmt.Sprintf("%f", e.value)
}

type scalarExpression struct {
	leftExpr  testExpression
	rightExpr testExpression
	op        string
}

func (e scalarExpression) String() string {
	return fmt.Sprintf("%s %s %s", e.leftExpr.String(), e.op, e.rightExpr.String())
}

type binaryExpression struct {
	leftExpr  testExpression
	rightExpr testExpression
	op        string
}

func (e binaryExpression) String() string {
	return fmt.Sprintf("%s %s %s", e.leftExpr.String(), e.op, e.rightExpr.String())
}

type queryExpression struct {
	query string
}

func (e queryExpression) String() string {
	return e.query
}

func genExpression(opts genExpressionOptions) gopter.Gen {
	switch opts.exprType {
	case scalarExpressionType:
		subOpts := opts
		subOpts.exprNum--
		if subOpts.exprNum <= 1 {
			// If at the end just return a single constant
			return genExpressionSingle(genExpressionSingleOptions{
				exprType: constantExpressionType,
			})
		}

		ops := []string{"+", "-", "*", "/"}
		return gopter.CombineGens(
			genExpressionSingle(genExpressionSingleOptions{
				exprType: constantExpressionType,
			}),
			gen.IntRange(0, len(ops)-1),
			genExpression(subOpts),
		).Map(func(input []interface{}) testExpression {
			leftExpr := input[0].(testExpression)
			op := ops[input[1].(int)]
			rightExpr := input[2].(testExpression)
			return scalarExpression{
				leftExpr:  leftExpr,
				op:        op,
				rightExpr: rightExpr,
			}
		})
	default:
		panic(fmt.Errorf("unsupported gen expr type: %v", opts.exprType))
	}
}

type genExpressionSingleOptions struct {
	exprType expressionType
}

func genExpressionSingle(opts genExpressionSingleOptions) gopter.Gen {
	switch opts.exprType {
	case constantExpressionType:
		return gopter.CombineGens(
			gen.Float64Range(0, 1000),
		).Map(func(input []interface{}) testExpression {
			value := input[0].(float64)
			return constantExpression{
				value: value,
			}
		})
	default:
		panic(fmt.Errorf("unsupported gen single expr type: %v", opts.exprType))
	}
}

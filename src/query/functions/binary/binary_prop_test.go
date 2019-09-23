package binary

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/block"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

func TestQuerySimplePropertyTest(t *testing.T) {
	processBothSeriesPropertyFunc := func(input propTestMultiInput) (bool, error) {

		return true, nil
	}

	parameters := gopter.DefaultTestParameters()
	parameters.Rng.Seed(123456789)
	parameters.MinSuccessfulTests = 10
	props := gopter.NewProperties(parameters)
	props.Property("Test processBothSeries",
		prop.ForAll(processBothSeriesPropertyFunc, genPropTestMultiInputs(propTestMultiInputsOptions{
			inputsOpts: []propTestInputsOptions{
				{
					stepCountMin: 100,
					stepCountMax: 200,
					stepSize:     15 * time.Second,
				},
			},
		})))

	props.TestingRun(t)
}

type propTestInput struct {
	stepIter block.StepIter
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
	stepCountMin int
	stepCountMax int
	stepSize     time.Duration
}

func genPropTestInputs(opts propTestInputsOptions) gopter.Gen {
	return gopter.CombineGens(
		gen.IntRange(opts.stepCountMin, opts.stepCountMax),
	).FlatMap(func(input interface{}) gopter.Gen {
		inputs := input.([]interface{})
		numSteps := inputs[0].(int)
		return genPropTestInput(genStepIterOptions{
			numSteps: numSteps,
			stepSize: opts.stepSize,
		})
	}, reflect.TypeOf(propTestInput{}))
}

func genPropTestInput(opts genStepIterOptions) gopter.Gen {
	return gopter.CombineGens(
		genStepIter(opts),
	).Map(func(inputs []interface{}) propTestInput {
		return propTestInput{
			stepIter: inputs[0].(*generatedStepIter),
		}
	})
}

type genStepIterOptions struct {
	numSteps int
	stepSize time.Duration
}

func genStepIter(opts genStepIterOptions) gopter.Gen {
	return gopter.CombineGens().FlatMap(func(input interface{}) gopter.Gen {
		return gen.Int64Range(math.MinInt64, math.MaxInt64).
			Map(func(input interface{}) *generatedStepIter {
				randSeed := input.(int64)
				randGen := rand.New(rand.NewSource(randSeed))
				return &generatedStepIter{
					randGen: generatedStepIter,
				}
			})
	}, reflect.TypeOf(&generatedStepIter{}))
}

type generatedStepIter struct {
	randGen        *rand.Rand
	currStepTime   time.Time
	currStepValues []float64
	stepsCount     int
	stepsRemaining int
	seriesMeta     []block.SeriesMeta
}

func (i *generatedStepIter) Next() bool {
	result := i.stepsRemaining > 0
	i.stepsRemaining--
	return result
}

func (i *generatedStepIter) Err() error {
	return nil
}

func (i *generatedStepIter) Close() {
}

func (i *generatedStepIter) SeriesMeta() []block.SeriesMeta {
	return i.seriesMeta
}

func (i *generatedStepIter) StepCount() int {
	return i.stepsCount
}

func (i *generatedStepIter) Current() block.Step {
	return i
}

func (i *generatedStepIter) Time() time.Time {
	return i.currStepTime
}

func (i *generatedStepIter) Values() []float64 {
	return i.currStepValues
}

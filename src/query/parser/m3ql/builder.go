package m3ql

type builder struct {
}

func (b *builder) newMacro(string) {
	panic("implement me")
}

func (b *builder) newPipeline() {
	panic("implement me")
}

func (b *builder) endPipeline() {
	panic("implement me")
}

func (b *builder) newExpression(string) {
	panic("implement me")
}

func (b *builder) endExpression() {
	panic("implement me")
}

func (b *builder) newBooleanArgument(string) {
	panic("implement me")
}

func (b *builder) newNumericArgument(string) {
	panic("implement me")
}

func (b *builder) newPatternArgument(string) {
	panic("implement me")
}

func (b *builder) newStringLiteralArgument(string) {
	panic("implement me")
}

func (b *builder) newKeywordArgument(string) {
	panic("implement me")
}

// Function represents a function of the expression language and is
// used by function nodes.
type Function struct {
	Name       string
	ArgTypes   []ValueType
	Variadic   bool
	ReturnType ValueType

	// vals is a list of the evaluated arguments for the function call.
	//    For range vectors it will be a Matrix with one series, instant vectors a
	//    Vector, scalars a Vector with one series whose value is the scalar
	//    value,and nil for strings.
	// args are the original arguments to the function, where you can access
	//    matrixSelectors, vectorSelectors, and StringLiterals.
	// enh.out is a pre-allocated empty vector that you may use to accumulate
	//    output before returning it. The vectors in vals should not be returned.a
	// Range vector functions need only return a vector with the right value,
	//     the metric and timestamp are not neded.
	// Instant vector functions need only return a vector with the right values and
	//     metrics, the timestamp are not needed.
	// Scalar results should be returned as the value of a sample in a Vector.
	Call     func(vals []Value, args Expressions, enh *EvalNodeHelper) Vector
	Defaults map[uint8]interface{}
}

// ValueType describes a type of a value.
type ValueType string

// The valid value types.
const (
	ValueTypeNone   = "none"
	ValueTypeVector = "vector"
	ValueTypeScalar = "scalar"
	ValueTypeMatrix = "matrix"
	ValueTypeString = "string"
)

// Value is a generic interface for values resulting from a query evaluation.
type Value interface {
	Type() ValueType
	String() string
}

func (Matrix) Type() ValueType { return ValueTypeMatrix }
func (Vector) Type() ValueType { return ValueTypeVector }
func (Scalar) Type() ValueType { return ValueTypeScalar }
func (String) Type() ValueType { return ValueTypeString }

// String represents a string value.
type String struct {
	V string
	T int64
}

// Scalar is a data point that's explicitly not associated with a metric.
type Scalar struct {
	T int64
	V float64
}

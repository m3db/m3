package promql

// ItemType which maps to ItemType in Prometheus. Note that this is temporary and
// once we decide we want to use this approach then we will just expose the item types in Prometheus code base
type ItemType int

// nolint: deadcode
const (
	itemError ItemType = iota // Error occurred, value is error message
	itemEOF
	itemComment
	itemIdentifier
	itemMetricIdentifier
	itemLeftParen
	itemRightParen
	itemLeftBrace
	itemRightBrace
	itemLeftBracket
	itemRightBracket
	itemComma
	itemAssign
	itemSemicolon
	itemString
	itemNumber
	itemDuration
	itemBlank
	itemTimes

	operatorsStart
	// Operators.
	itemSUB
	itemADD
	itemMUL
	itemMOD
	itemDIV
	itemLAND
	itemLOR
	itemLUnless
	itemEQL
	itemNEQ
	itemLTE
	itemLSS
	itemGTE
	itemGTR
	itemEQLRegex
	itemNEQRegex
	itemPOW
	operatorsEnd

	aggregatorsStart
	// Aggregators.
	itemAvg
	itemCount
	itemSum
	itemMin
	itemMax
	itemStddev
	itemStdvar
	itemTopK
	itemBottomK
	itemCountValues
	itemQuantile
	aggregatorsEnd

	keywordsStart
	// Keywords.
	itemAlert
	itemIf
	itemFor
	itemLabels
	itemAnnotations
	itemOffset
	itemBy
	itemWithout
	itemOn
	itemIgnoring
	itemGroupLeft
	itemGroupRight
	itemBool
	keywordsEnd
)

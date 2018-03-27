package changes

// Op is an update operation.
type Op string

// A list of supported update operations.
const (
	AddOp    Op = "add"
	ChangeOp Op = "change"
	DeleteOp Op = "delete"
)

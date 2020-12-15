package limits

import xerrors "github.com/m3db/m3/src/x/errors"

type queryLimitExceededError struct {
	msg string
}

// NewQueryLimitExceededError creates a query limit exceeded error.
func NewQueryLimitExceededError(msg string) error {
	return &queryLimitExceededError{
		msg: msg,
	}
}

func (err *queryLimitExceededError) Error() string {
	return err.msg
}

// IsQueryLimitExceededError returns true if the error is a query limits exceeded error.
func IsQueryLimitExceededError(err error) bool {
	for err != nil {
		if _, ok := err.(*queryLimitExceededError); ok { //nolint:errorlint
			return true
		}
		err = xerrors.InnerError(err)
	}
	return false
}

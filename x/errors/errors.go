package errors

// FirstError will return the first non nil error
func FirstError(errs ...error) error {
	for i := range errs {
		if errs[i] != nil {
			return errs[i]
		}
	}
	return nil
}

type containedError struct {
	inner error
}

type containedErr interface {
	innerError() error
}

// InnerError returns the packaged inner error if this is an error that contains another
func InnerError(err error) error {
	contained, ok := err.(containedErr)
	if !ok {
		return nil
	}
	return contained.innerError()
}

type invalidParamsError struct {
	containedError
}

// NewInvalidParamsError creates a new invalid params error
func NewInvalidParamsError(inner error) error {
	return invalidParamsError{containedError{inner}}
}

func (e invalidParamsError) Error() string {
	return e.inner.Error()
}

func (e invalidParamsError) innerError() error {
	return e.inner
}

// IsInvalidParams returns true if this is an invalid params error
func IsInvalidParams(err error) bool {
	return GetInnerInvalidParamsError(err) != nil
}

// GetInnerInvalidParamsError returns an inner invalid params error
// if contained by this error, nil otherwise
func GetInnerInvalidParamsError(err error) error {
	for err != nil {
		if _, ok := err.(invalidParamsError); ok {
			return InnerError(err)
		}
		err = InnerError(err)
	}
	return nil
}

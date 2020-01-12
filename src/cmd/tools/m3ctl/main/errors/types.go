package errors

type FlagsError struct {
	Message string
}

func (e *FlagsError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

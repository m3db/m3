package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRetryableError(t *testing.T) {
	var (
		innerErr = errors.New("inner")
		err      = NewRetryableError(innerErr)
	)

	assert.Error(t, err)
	assert.True(t, IsRetryable(err))
	assert.Equal(t, innerErr, GetInnerRetryableError(err))
}

func TestInvalidParamsError(t *testing.T) {
	var (
		innerErr = errors.New("inner")
		err      = NewInvalidParamsError(innerErr)
	)

	assert.Error(t, err)
	assert.True(t, IsInvalidParams(err))
	assert.Equal(t, innerErr, GetInnerInvalidParamsError(err))
}

func TestDeprecatedError(t *testing.T) {
	var (
		innerErr = errors.New("inner")
		err      = NewDeprecatedError(innerErr)
	)

	assert.Error(t, err)
	assert.True(t, IsDeprecated(err))
	assert.Equal(t, innerErr, GetInnerDeprecatedError(err))
}

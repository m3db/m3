package instrument

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLoggerContext(t *testing.T) {
	l := zap.NewNop()
	ctx := NewContextFromLogger(context.Background(), l)
	ctxL := LoggerFromContext(ctx)
	require.Equal(t, l, ctxL)
	require.Equal(t, l, NewOptions().LoggerFromContext(ctx))
}

func TestNilLoggerContext(t *testing.T) {
	ctx := NewContextFromLogger(context.Background(), nil)
	require.Nil(t, LoggerFromContext(ctx))
	require.NotNil(t, NewOptions().LoggerFromContext(ctx))
}

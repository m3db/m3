package xtchannel

import (
	"testing"

	"github.com/uber/tchannel-go"
)

func TestNoopLogger(t *testing.T) {
	logger := NewNoopLogger()

	// Test Enabled always returns false
	if logger.Enabled(tchannel.LogLevelDebug) {
		t.Error("Expected Enabled to return false for Debug")
	}
	if logger.Enabled(tchannel.LogLevelError) {
		t.Error("Expected Enabled to return false for Error")
	}

	// Check other methods (no panic expected)
	logger.Error("test error")
	logger.Warn("test warn")
	logger.Info("test info")
	logger.Infof("info %s", "formatted")
	logger.Debug("test debug")
	logger.Debugf("debug %d", 1)

	if logger.Fields() != nil {
		t.Error("Expected Fields to return nil")
	}

	withFieldsLogger := logger.WithFields(tchannel.LogField{Key: "k", Value: "v"})
	if withFieldsLogger != logger {
		t.Error("Expected WithFields to return same logger")
	}
}

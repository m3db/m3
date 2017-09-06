package util

import (
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3x/log"
)

// ValidateFn validates an update from KV.
type ValidateFn func(interface{}) error

type getValueFn func(kv.Value) (interface{}, error)

type updateFn func(interface{})

// Options is a set of options for kv utility functions.
type Options interface {
	// SetValidateFn sets the validation function applied to kv values.
	SetValidateFn(val ValidateFn) Options

	// ValidateFn returns the validation function applied to kv values.
	ValidateFn() ValidateFn

	// SetLogger sets the logger.
	SetLogger(val xlog.Logger) Options

	// Logger returns the logger.
	Logger() xlog.Logger
}

type options struct {
	validateFn ValidateFn
	logger     xlog.Logger
}

// NewOptions returns a new set of options for kv utility functions.
func NewOptions() Options {
	return &options{}
}

func (o *options) SetValidateFn(val ValidateFn) Options {
	opts := *o
	opts.validateFn = val
	return &opts
}

func (o *options) ValidateFn() ValidateFn {
	return o.validateFn
}

func (o *options) SetLogger(val xlog.Logger) Options {
	opts := *o
	opts.logger = val
	return &opts
}

func (o *options) Logger() xlog.Logger {
	return o.logger
}

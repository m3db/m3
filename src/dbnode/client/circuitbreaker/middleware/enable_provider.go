package middleware

import (
	"fmt"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/dbnode/generated/proto/circuitbreaker"
)

// EnableConfig represents the configuration stored in etcd.
type EnableConfig struct {
	Enabled    bool `yaml:"enabled"`
	ShadowMode bool `yaml:"shadowMode"`
}

const (
	// configPath is the path where the circuit breaker middleware configuration is stored.
	_configPath = "circuitbreaker/middleware/config"
)

// EnableProvider defines the interface for checking if the circuit breaker is enabled.
type EnableProvider interface {
	// IsEnabled returns whether the circuit breaker is enabled.
	IsEnabled() bool
	// IsShadowMode returns whether the circuit breaker is in shadow mode.
	IsShadowMode() bool
	// WatchConfig watches for changes to the circuit breaker middleware configuration.
	WatchConfig(store kv.Store, logger *zap.Logger) error
}

// enableProvider implements the EnableProvider interface.
type enableProvider struct {
	configValue atomic.Value
}

// NewEnableProvider creates a new enable provider.
func NewEnableProvider() EnableProvider {
	return &enableProvider{}
}

// NewNopEnableProvider creates a new nop enable provider.
func NewNopEnableProvider() EnableProvider {
	return &nopEnableProvider{}
}

// nopEnableProvider is a nop enable provider.
type nopEnableProvider struct{}

// IsEnabled returns whether the circuit breaker is enabled.
func (p *enableProvider) IsEnabled() bool {
	if v := p.configValue.Load(); v != nil {
		config := v.(EnableConfig)
		return config.Enabled
	}
	return false
}

// IsShadowMode returns whether the circuit breaker is in shadow mode.
func (p *enableProvider) IsShadowMode() bool {
	if v := p.configValue.Load(); v != nil {
		config := v.(EnableConfig)
		return config.ShadowMode
	}
	return false
}

// WatchConfig watches for changes to the circuit breaker middleware configuration in etcd.
func (p *enableProvider) WatchConfig(store kv.Store, logger *zap.Logger) error {
	logger.Info("watching circuit breaker middleware configuration")

	watch, err := store.Watch(_configPath)
	if err != nil {
		return fmt.Errorf("failed to watch circuit breaker middleware configuration: %w", err)
	}

	logger.Info("watch created for circuit breaker middleware configuration")

	go func() {
		for range watch.C() {
			// Get the current value
			value, err := store.Get(_configPath)
			if err != nil {
				logger.Error("failed to get circuit breaker middleware configuration", zap.Error(err))
				continue
			}
			logger.Info("circuit breaker middleware configuration changed", zap.Any("value", value))

			// Unmarshal into EnableConfigProto
			var configProto circuitbreaker.EnableConfigProto
			if err := value.Unmarshal(&configProto); err != nil {
				logger.Error("failed to unmarshal circuit breaker middleware configuration", zap.Error(err))
				continue
			}

			// Create a new config with the boolean flags from etcd
			config := EnableConfig{
				Enabled:    configProto.Enabled,
				ShadowMode: configProto.ShadowMode,
			}

			// Store the config in atomic value
			p.configValue.Store(config)
		}
	}()
	logger.Info("watching circuit breaker middleware configuration complete")
	return nil
}

// IsEnabled returns whether the circuit breaker is enabled.
func (p *nopEnableProvider) IsEnabled() bool {
	return false
}

// IsShadowMode returns whether the circuit breaker is in shadow mode.
func (p *nopEnableProvider) IsShadowMode() bool {
	return false
}

// WatchConfig is a nop implementation.
func (p *nopEnableProvider) WatchConfig(store kv.Store, logger *zap.Logger) error {
	return nil
}

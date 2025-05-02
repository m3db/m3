package middleware

import (
	"fmt"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"github.com/m3db/m3/src/cluster/kv"
	"go.uber.org/zap"
)

// EtcdConfig represents the configuration stored in etcd.
type EtcdConfig struct {
	Enabled    bool `yaml:"enabled"`
	ShadowMode bool `yaml:"shadowMode"`
}

// EtcdConfigProto is a protobuf message for the etcd config.
type EtcdConfigProto struct {
	Enabled    bool `protobuf:"varint,1,opt,name=enabled,proto3" json:"enabled,omitempty"`
	ShadowMode bool `protobuf:"varint,2,opt,name=shadow_mode,json=shadowMode,proto3" json:"shadow_mode,omitempty"`
}

func (m *EtcdConfigProto) Reset()         { *m = EtcdConfigProto{} }
func (m *EtcdConfigProto) String() string { return proto.CompactTextString(m) }
func (*EtcdConfigProto) ProtoMessage()    {}

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

// IsEnabled returns whether the circuit breaker is enabled.
func (p *enableProvider) IsEnabled() bool {
	if v := p.configValue.Load(); v != nil {
		config := v.(EtcdConfig)
		return config.Enabled
	}
	return false
}

// IsShadowMode returns whether the circuit breaker is in shadow mode.
func (p *enableProvider) IsShadowMode() bool {
	if v := p.configValue.Load(); v != nil {
		config := v.(EtcdConfig)
		return config.ShadowMode
	}
	return false
}

// WatchConfig watches for changes to the circuit breaker middleware configuration in etcd.
func (p *enableProvider) WatchConfig(store kv.Store, logger *zap.Logger) error {
	logger.Info("watching circuit breaker middleware configuration")

	watch, err := store.Watch("circuitbreaker/middleware/config")
	if err != nil {
		return fmt.Errorf("failed to watch circuit breaker middleware configuration: %v", err)
	}

	logger.Info("watch created for circuit breaker middleware configuration")

	go func() {
		for {
			select {
			case <-watch.C():
				// Get the current value
				value, err := store.Get("circuitbreaker/middleware/config")
				if err != nil {
					logger.Error("failed to get circuit breaker middleware configuration", zap.Error(err))
					continue
				}
				logger.Info("circuit breaker middleware configuration changed", zap.Any("value", value))

				// Unmarshal into EtcdConfigProto
				var configProto EtcdConfigProto
				if err := value.Unmarshal(&configProto); err != nil {
					logger.Error("failed to unmarshal circuit breaker middleware configuration", zap.Error(err))
					continue
				}

				// Create a new config with the boolean flags from etcd
				config := EtcdConfig{
					Enabled:    configProto.Enabled,
					ShadowMode: configProto.ShadowMode,
				}

				// Store the config in atomic value
				p.configValue.Store(config)
			}
		}
	}()
	logger.Info("watching circuit breaker middleware configuration complete")
	return nil
}

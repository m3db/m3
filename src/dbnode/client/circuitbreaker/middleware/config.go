package middleware

import (
	"fmt"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/dbnode/client/circuitbreaker"
	"go.uber.org/zap"
)

// Config represents the configuration for the circuit breaker middleware.
type Config struct {
	CircuitBreakerConfig circuitbreaker.Config `yaml:"circuitBreakerConfig"`
}

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

var (
	configValue atomic.Value
)

// IsEnabled returns whether the circuit breaker is enabled.
func IsEnabled() bool {
	if v := configValue.Load(); v != nil {
		config := v.(EtcdConfig)
		return config.Enabled
	}
	return false
}

// IsShadowMode returns whether the circuit breaker is in shadow mode.
func IsShadowMode() bool {
	if v := configValue.Load(); v != nil {
		config := v.(EtcdConfig)
		return config.ShadowMode
	}
	return false
}

// WatchConfig watches for changes to the circuit breaker middleware configuration in etcd.
// It takes a kv store and logger.
func WatchConfig(
	store kv.Store,
	logger *zap.Logger,
) error {
	// Watch for changes to the circuit breaker middleware configuration

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
				configValue.Store(config)
			}
		}
	}()
	logger.Info("watching circuit breaker middleware configuration complete")
	return nil
}

package namespace

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRuntimeOptions_DefaultAndEquality(t *testing.T) {
	opts := NewRuntimeOptions()

	assert.True(t, opts.IsDefault())
	assert.True(t, opts.Equal(NewRuntimeOptions()))

	write := 0.9
	flush := 0.2

	updated := opts.SetWriteIndexingPerCPUConcurrency(&write).
		SetFlushIndexingPerCPUConcurrency(&flush)

	assert.False(t, updated.IsDefault())
	assert.False(t, updated.Equal(opts))
	assert.Equal(t, write, updated.WriteIndexingPerCPUConcurrencyOrDefault())
	assert.Equal(t, flush, updated.FlushIndexingPerCPUConcurrencyOrDefault())
}

func TestRuntimeOptions_DefaultValues(t *testing.T) {
	opts := NewRuntimeOptions()

	assert.Equal(t, defaultWriteIndexingPerCPUConcurrency, opts.WriteIndexingPerCPUConcurrencyOrDefault())
	assert.Equal(t, defaultFlushIndexingPerCPUConcurrency, opts.FlushIndexingPerCPUConcurrencyOrDefault())
}

func TestRuntimeOptionsManager_UpdateAndGet(t *testing.T) {
	manager := NewRuntimeOptionsManager("test-ns")
	defaultOpts := manager.Get()
	assert.True(t, defaultOpts.IsDefault())

	newWrite := 1.2
	newOpts := NewRuntimeOptions().SetWriteIndexingPerCPUConcurrency(&newWrite)

	err := manager.Update(newOpts)
	assert.NoError(t, err)

	latest := manager.Get()
	assert.Equal(t, newWrite, latest.WriteIndexingPerCPUConcurrencyOrDefault())
}

type mockRuntimeListener struct {
	mu   sync.Mutex
	opts []RuntimeOptions
}

func (l *mockRuntimeListener) SetNamespaceRuntimeOptions(value RuntimeOptions) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.opts = append(l.opts, value)
}

func (l *mockRuntimeListener) Values() []RuntimeOptions {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.opts
}

func TestRuntimeOptionsManager_RegisterListener(t *testing.T) {
	manager := NewRuntimeOptionsManager("test-ns")
	listener := &mockRuntimeListener{}
	closer := manager.RegisterListener(listener)

	time.Sleep(10 * time.Millisecond)

	newFlush := 0.99
	err := manager.Update(NewRuntimeOptions().SetFlushIndexingPerCPUConcurrency(&newFlush))
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	closer.Close()

	values := listener.Values()
	assert.Equal(t, len(values), 2)
	assert.Equal(t, newFlush, values[len(values)-1].FlushIndexingPerCPUConcurrencyOrDefault())
}

func TestRuntimeOptionsManagerRegistry(t *testing.T) {
	reg := NewRuntimeOptionsManagerRegistry()

	manager1 := reg.RuntimeOptionsManager("ns1")
	manager2 := reg.RuntimeOptionsManager("ns1")
	assert.Equal(t, manager1, manager2)

	manager3 := reg.RuntimeOptionsManager("ns2")
	assert.NotEqual(t, manager1, manager3)

	reg.Close()

	manager4 := reg.RuntimeOptionsManager("ns1")
	assert.NotEqual(t, manager1, manager4)
}

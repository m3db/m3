package remote

import (
	"google.golang.org/grpc/naming"
)

type staticResolver struct {
	updates []*naming.Update
}

func newStaticResolver(addresses []string) naming.Resolver {
	var updates []*naming.Update
	for _, address := range addresses {
		updates = append(updates, &naming.Update{
			Op:       naming.Add,
			Addr:     address,
			Metadata: nil,
		})
	}
	return &staticResolver{
		updates: updates,
	}
}

// Resolve creates a Watcher for target.
func (r *staticResolver) Resolve(target string) (naming.Watcher, error) {
	ch := make(chan []*naming.Update, 1)
	ch <- r.updates
	return &staticWatcher{
		updates: ch,
	}, nil
}

type staticWatcher struct {
	updates chan []*naming.Update
}

// Next returns the static address set
func (w *staticWatcher) Next() ([]*naming.Update, error) {
	return <-w.updates, nil
}

// Close closes the watcher
func (w *staticWatcher) Close() {
	close(w.updates)
}

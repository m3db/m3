package election

import "github.com/coreos/etcd/clientv3/concurrency"

type clientOpts struct {
	sessionOpts []concurrency.SessionOption
}

// ClientOption provides a means of configuring optional parameters for a
// client.
type ClientOption func(*clientOpts)

// WithSessionOptions sets the options passed to all underlying
// concurrency.Session instances associated with elections. If the user wishes
// to override the TTL of sessions, concurrency.WithTTL(ttl) should be passed
// here.
func WithSessionOptions(opts ...concurrency.SessionOption) ClientOption {
	return func(o *clientOpts) {
		o.sessionOpts = opts
	}
}

package tchannelthrift

import (
	"code.uber.internal/infra/memtsdb/services/mdbnode/serve"
	"code.uber.internal/infra/memtsdb/services/mdbnode/serve/tchannelthrift/thrift/gen-go/rpc"
	"code.uber.internal/infra/memtsdb/services/mdbnode/storage"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

const (
	// ChannelName is the name of the TChannel channel name the service is exposed on
	ChannelName = "Node"
)

type server struct {
	address string
	opts    *tchannel.ChannelOptions
	db      storage.Database
}

// NewServer creates a TChannel Thrift network service
func NewServer(
	db storage.Database,
	address string,
	opts *tchannel.ChannelOptions,
) serve.NetworkService {
	// Make the opts immutable on the way in
	if opts != nil {
		immutableOpts := *opts
		opts = &immutableOpts
	}
	return &server{
		address: address,
		opts:    opts,
		db:      db,
	}
}

func (s *server) ListenAndServe() (serve.Close, error) {
	channel, err := tchannel.NewChannel(ChannelName, s.opts)
	if err != nil {
		return nil, err
	}

	server := thrift.NewServer(channel)
	server.Register(rpc.NewTChanNodeServer(NewService(s.db)))

	channel.ListenAndServe(s.address)

	return channel.Close, nil
}

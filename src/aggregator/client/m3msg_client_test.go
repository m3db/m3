package client

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/msg/producer"
)

func TestNewM3MsgClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	p := producer.NewMockProducer(ctrl)
	p.EXPECT().Init()
	p.EXPECT().NumShards().Return(uint32(1))

	opts := NewM3MsgOptions().
		SetProducer(p)

	c, err := NewM3MsgClient(NewOptions().SetM3MsgOptions(opts))
	assert.NotNil(t, c)
	assert.NoError(t, err)
}

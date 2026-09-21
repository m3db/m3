package convert

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	m3db "github.com/m3db/m3/src/dbnode/x/m3em/node"
	m3emnode "github.com/m3db/m3/src/m3em/node"
)

func TestAsNodes(t *testing.T) {
	tests := []struct {
		name        string
		input       func(ctrl *gomock.Controller) []m3emnode.ServiceNode
		expectedIDs []string
		expectError bool
	}{
		{
			name: "all valid nodes",
			input: func(ctrl *gomock.Controller) []m3emnode.ServiceNode {
				n := m3db.NewMockNode(ctrl)
				n.EXPECT().String().Return("node1")
				return []m3emnode.ServiceNode{n}
			},
			expectError: false,
			expectedIDs: []string{"node1"},
		},
		{
			name: "contains non-m3emnode",
			input: func(ctrl *gomock.Controller) []m3emnode.ServiceNode {
				n := m3emnode.NewMockServiceNode(ctrl)
				n.EXPECT().String().Return("node1")
				return []m3emnode.ServiceNode{n}
			},
			expectedIDs: nil,
			expectError: true,
		},
		{
			name: "empty slice",
			input: func(ctrl *gomock.Controller) []m3emnode.ServiceNode {
				return []m3emnode.ServiceNode{}
			},
			expectedIDs: []string{},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			result, err := AsNodes(tt.input(ctrl))
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Len(t, result, len(tt.expectedIDs))
				for i, id := range tt.expectedIDs {
					assert.Equal(t, id, result[i].String())
				}
			}
		})
	}
}

func TestAsServiceNodes(t *testing.T) {
	tests := []struct {
		name        string
		input       func(ctrl *gomock.Controller) []m3db.Node
		expectedIDs []string
		expectError bool
	}{
		{
			name: "all valid nodes",
			input: func(ctrl *gomock.Controller) []m3db.Node {
				n := m3db.NewMockNode(ctrl)
				n.EXPECT().String().Return("node1")
				return []m3db.Node{n}
			},
			expectedIDs: []string{"node1"},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			result, err := AsServiceNodes(tt.input(ctrl))
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Len(t, result, len(tt.expectedIDs))
				for i, id := range tt.expectedIDs {
					assert.Equal(t, id, result[i].String())
				}
			}
		})
	}
}

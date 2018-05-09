package local

import (
	"time"

	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/storage/local"

	"github.com/m3db/m3db/client"

	"github.com/golang/mock/gomock"
)

// NewStorageAndSession generates a new local storage and mock session
func NewStorageAndSession(ctrl *gomock.Controller) (storage.Storage, *client.MockSession) {
	session := client.NewMockSession(ctrl)
	storage := local.NewStorage(session, "metrics", time.Minute)
	return storage, session
}

package checkArgs

import "go.uber.org/zap"

type GlobalOpts struct {
	Endpoint string
	zap      *zap.SugaredLogger
}

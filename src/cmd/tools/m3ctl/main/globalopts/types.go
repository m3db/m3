package globalopts

import "go.uber.org/zap"

type GlobalOpts struct {
	Endpoint string
	Zap      *zap.SugaredLogger
}

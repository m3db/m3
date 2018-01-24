package models

import (
	"time"
)

// ReadQuery represents the input query which is fetched from M3DB
type ReadQuery struct {
	TagMatchers Matchers
	Start time.Time
	End time.Time
}

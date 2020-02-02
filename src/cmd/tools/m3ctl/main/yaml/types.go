package yaml

import "github.com/m3db/m3/src/query/generated/proto/admin"

type OP int
const (
	Create OP = iota
)

type QQQ struct {
	operation OP
	admin.DatabaseCreateRequest
}

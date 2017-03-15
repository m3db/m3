//go:generate protoc --go_out=plugins=grpc:. m3nsch.proto
//go:generate mockgen -source=m3nsch.pb.go -package=rpc -destination=mock_m3nsch.pb.go

package rpc

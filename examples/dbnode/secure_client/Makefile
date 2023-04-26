ROOT_DIR := ../../../
BIN_DIR := ${ROOT_DIR}/bin
GO_BUILD_FLAGS := CGO_ENABLED=0 GOOS=linux GOARCH=arm64

.PHONY: default
default: all;

.PHONY: m3coordinator
m3coordinator:
	@echo Building OSS Service: m3coordinator
	cd ${ROOT_DIR} && ${GO_BUILD_FLAGS} make m3coordinator

	@echo Building m3coordinator:dev docker image
	docker build -t m3coordinator:dev -f docker/coordinator.Dockerfile ${BIN_DIR}
	@echo Built m3coordinator:dev docker image


.PHONY: m3dbnode
m3dbnode:
	@echo Building OSS Service: m3dbnode
	cd ${ROOT_DIR} && ${GO_BUILD_FLAGS} make m3dbnode

	@echo Building m3dbnode:dev docker image
	docker build -t m3dbnode:dev -f docker/dbnode.Dockerfile ${BIN_DIR}
	@echo Built m3dbnode:dev docker image


.PHONY: m3dbclient
m3dbclient:
	@echo Bazel building m3db:benchmark-client docker image base
	${GO_BUILD_FLAGS} go build -o ${BIN_DIR}/m3db-client .

	@echo Building m3dbclient:dev docker image
	docker build -t m3dbclient:dev -f docker/client.Dockerfile ${BIN_DIR}
	@echo Built m3dbclient:dev docker image

.PHONY: all
all: m3coordinator m3dbnode m3dbclient


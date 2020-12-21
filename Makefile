ROOT:=$(shell pwd)
GIT_SHA=$(shell git rev-parse --short HEAD || echo "GitNotFound")
BUILD_TIME=$(shell date "+%Y-%m-%d_%H:%M:%S")
GO_LDFLAGS="-X github.com/zbdba/db-recovery/recovery/client.GitSHA=${GIT_SHA} -X github.com/zbdba/db-recovery/recovery/client.BuildTime=${BUILD_TIME}"

all: build

build: db-recovery
db-recovery: 
	go build -ldflags ${GO_LDFLAGS} -o ./bin/db-recovery ./cmd/recovery/main.go
clean:
	@rm -rf ${ROOT}/bin

format:
	gofmt -w cmd/ parse_file/

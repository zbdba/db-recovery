ROOT:=$(shell pwd)
GIT_SHA=$(shell git rev-parse --short HEAD || echo "GitNotFound")
BUILD_TIME=$(shell date "+%Y-%m-%d_%H:%M:%S")
GO_LDFLAGS="-X github.com/zbdba/db-recovery/cmd/recovery/main.GitSHA=${GIT_SHA} -X github.com/zbdba/db-recovery/cmd/recovery/main.BuildTime=${BUILD_TIME}"

all: build

build: db-recovery

.PHONY:db-recovery
db-recovery:
	@mkdir -p bin
	@ret=0 && for d in $$(go list -f '{{if (eq .Name "main")}}{{.ImportPath}}{{end}}' ./... | grep -v test); do \
		b=$$(basename $${d}) ; \
		go build -ldflags ${GO_LDFLAGS} -o bin/$${b} $$d || ret=$$? ; \
	done ; exit $$ret

.PHONY: clean
clean:
	@rm -rf ${ROOT}/bin

.PHONY: fmt
fmt:
	go fmt ./...

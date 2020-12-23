ROOT:=$(shell pwd)
GIT_SHA=$(shell git rev-parse --short HEAD || echo "GitNotFound")
BUILD_TIME=$(shell date "+%Y-%m-%d_%H:%M:%S")
GO_LDFLAGS="-X github.com/zbdba/db-recovery/cmd/recovery/main.GitSHA=${GIT_SHA} -X github.com/zbdba/db-recovery/cmd/recovery/main.BuildTime=${BUILD_TIME}"

# Add mysql version for testing `MYSQL_RELEASE=percona MYSQL_VERSION=5.7 make docker`
# MySQL 5.1 `MYSQL_RELEASE=vsamov/mysql-5.1.73 make docker`
# MYSQL_RELEASE: mysql, percona, mariadb ...
# MYSQL_VERSION: latest, 8.0, 5.7, 5.6, 5.5 ...
# use mysql:latest as default
MYSQL_RELEASE := $(or ${MYSQL_RELEASE}, ${MYSQL_RELEASE}, mysql)
MYSQL_VERSION := $(or ${MYSQL_VERSION}, ${MYSQL_VERSION}, 5.7)

DOCKER_VOLUME=$(shell docker inspect recovery-mysql | jq -r '.[] | .Mounts | .[] | select(.Type == "volume") | .Source')

all: build

build: fmt db-recovery

.PHONY:db-recovery
db-recovery:
	@mkdir -p bin
	@echo "go build ./..."
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

.PHONY: docker-mysql
docker-mysql:
	@docker stop recovery-mysql 2>/dev/null || true
	@docker wait recovery-mysql 2>/dev/null >/dev/null || true
	@echo "docker run --name recovery-mysql $(MYSQL_RELEASE):$(MYSQL_VERSION)"
	@docker run --name recovery-mysql --rm -d \
	-e MYSQL_ROOT_PASSWORD=123456 \
	-e MYSQL_DATABASE=test \
	-p 3306:3306 \
	-v `pwd`/cmd/test/test.sql:/docker-entrypoint-initdb.d/test.sql \
	-v `pwd`/cmd/test/fixture/$(MYSQL_RELEASE)_$(MYSQL_VERSION):/var/lib/mysql \
	$(MYSQL_RELEASE):$(MYSQL_VERSION) \
	--sql-mode ""
	@echo "waiting for earth database initializing "
	@timeout=180; while [ $${timeout} -gt 0 ] ; do \
	if ! docker exec recovery-mysql mysql --user=root --password=123456 --host "127.0.0.1" --silent -NBe "do 1" >/dev/null 2>&1 ; then \
			timeout=`expr $$timeout - 1`; \
			printf '.' ;  sleep 1 ; \
	else \
			echo "." ; echo "mysql test environment is ready!" ; break ; \
	fi ; \
	if [ $$timeout = 0 ] ; then \
			echo "." ; echo "docker recovery-mysql start timeout(180 s)!" ; exit 1 ; \
	fi ; \
	done

.PHONY: docker-connect
docker-connect:
	@docker exec -it recovery-mysql env LANG=C.UTF-8 mysql --user=root --password=123456 --host "127.0.0.1" test

.PHONY: test
test:
	@echo "Run all test cases ..."
	@go test -timeout 10m -race ./recovery/... -mysql-release $(MYSQL_RELEASE) -mysql-version $(MYSQL_VERSION)

	@ret=0 && for d in $$(go list -f '{{if (eq .Name "main")}}{{.ImportPath}}{{end}}' ./... | grep test); do \
		b=$$(basename $${d}) ; \
		go build -ldflags ${GO_LDFLAGS} -o bin/test-$${b} $$d || ret=$$? ; \
	done ; exit $$ret

	@echo "test Success!"

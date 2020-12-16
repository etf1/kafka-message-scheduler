VERSION ?= $(shell git describe --always --abbrev=1 --tags --match "v[0-9]*")
LDFLAGS=-ldflags "-X main.version=${VERSION}"
TMPDIR := $(shell mktemp -d)

dev.up:
	docker-compose -p dev up -d kafka

dev.down:
	docker-compose -p dev down -v

build:
	go build -tags musl -v ./...

.PHONY: bin
bin:
	go build ${LDFLAGS} -tags musl -v -o bin/kafka-scheduler ./cmd/kafka

run:
	go run ${LDFLAGS} -tags musl -v ./cmd/kafka

lint:
	golangci-lint --build-tags musl run

tests: #build lint
	echo "WWWWWWWWWW"
	cat "/proc/self/cgroup"
	go test -v -tags musl -race -count=1 ./... -run=^NONE 

tests.docker:
	docker-compose -p tests build tests
	docker-compose -p tests up tests | tee ${TMPDIR}/tests.result
	docker-compose -p tests down
	bash ./scripts/check_gotest.sh ${TMPDIR}/tests.result

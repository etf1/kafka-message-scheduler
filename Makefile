VERSION ?= $(shell git describe --always --abbrev=1 --tags --match "v[0-9]*")
LDFLAGS=-ldflags "-s -w -X main.Version=${VERSION}"
TMPDIR := $(shell mktemp -d)

up:
	docker-compose -p dev up -d kafka

down:
	docker-compose -p dev down -v

build:
	go build -tags musl -v ./...

builds:
	$(MAKE) build
	cd clientlib && $(MAKE) build -f ../Makefile

.PHONY: bin
bin:
	go build ${LDFLAGS} -tags musl -v -o bin/scheduler ./cmd/kafka

.PHONY: mock
mini:
	go build ${LDFLAGS} -tags musl -v -o bin/mini ./cmd/mini

run.mini:
	go run ${LDFLAGS} -tags musl -v ./cmd/mini

run:
	go run ${LDFLAGS} -tags musl -v ./cmd/kafka

lint:
	golangci-lint --build-tags musl run

lints:
	$(MAKE) lint
	cd clientlib && $(MAKE) lint -f ../Makefile

test:
	go test -v -tags musl -race -count=1 ./...

tests: builds lints
	$(MAKE) test
	cd clientlib && $(MAKE) test -f ../Makefile

tests.docker:
	docker-compose -p tests build tests
	docker-compose -p tests up tests | tee ${TMPDIR}/tests.result
	docker-compose -p tests down
	bash ./scripts/check_gotest.sh ${TMPDIR}/tests.result

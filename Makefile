VERSION ?= $(shell git describe --always --abbrev=1 --tags --match "v[0-9]*")
LDFLAGS=-ldflags "-X main.version=${VERSION}"
TMPDIR := $(shell mktemp -d)

dev.up:
	docker-compose -p dev up -d kafka

dev.down:
	docker-compose -p dev down -v

build:
	go build -tags musl -v ./...

builds:
	$(MAKE) build
	cd clientlib && $(MAKE) build -f ../Makefile

.PHONY: bin
bin:
	go build ${LDFLAGS} -tags musl -v -o bin/kafka-scheduler ./cmd/kafka

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

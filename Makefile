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
	go test -v -tags musl -failfast -race -count=1 ./... -coverprofile=./coverage/coverage.txt -covermode=atomic

tests: builds lints
	$(MAKE) test
	# cd clientlib && $(MAKE) test -f ../Makefile

tests.docker:
	docker-compose -p testsenv build tests; \
	docker-compose -p testsenv up --exit-code-from tests tests; ret=$$?; \
	docker-compose -p testsenv down -v; exit $$ret;

docker:
	docker build -t etf1/kafka-message-scheduler:${VERSION} -f ./cmd/kafka/Dockerfile .

docker.mini:
	docker build -t etf1/kafka-message-scheduler:mini-${VERSION} -f ./cmd/mini/Dockerfile .

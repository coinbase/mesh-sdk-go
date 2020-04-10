.PHONY: deps gen lint format check-format test test-coverage add-license \
	check-license shorten-lines shellcheck salus release
LICENCE_SCRIPT=addlicense -c "Coinbase, Inc." -l "apache" -v
TEST_SCRIPT=go test -v ./asserter/... ./fetcher/... ./models/... ./client/... ./server/...

deps:
	go get ./...
	go get github.com/stretchr/testify
	go get github.com/davecgh/go-spew
	go get github.com/google/addlicense
	go get github.com/segmentio/golines
	go get github.com/mattn/goveralls

gen:
	./codegen.sh

lint-examples:
	cd examples; \
	golangci-lint run -v \
		-E golint,misspell,gocyclo,gocritic,whitespace,goconst,gocognit,bodyclose,unconvert,lll,unparam

lint: | lint-examples
	golangci-lint run -v \
		-E golint,misspell,gocyclo,gocritic,whitespace,goconst,gocognit,bodyclose,unconvert,lll,unparam,gomnd

format:
	gofmt -s -w -l .

check-format:
	! gofmt -s -l . | read

test:
	${TEST_SCRIPT}

test-cover:	
	${TEST_SCRIPT} -coverprofile=c.out -covermode=count
	goveralls -coverprofile=c.out -repotoken ${COVERALLS_TOKEN}

add-license:
	${LICENCE_SCRIPT} .

check-license:
	${LICENCE_SCRIPT} -check .

shorten-lines:
	golines -w --shorten-comments asserter fetcher models client server examples

shellcheck:
	shellcheck codegen.sh

salus:
	docker run --rm -t -v ${PWD}:/home/repo coinbase/salus

release: shellcheck gen add-license shorten-lines format test lint salus

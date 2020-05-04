.PHONY: gen-deps deps gen lint format check-format test test-coverage add-license \
	check-license shorten-lines shellcheck salus release
LICENCE_SCRIPT=addlicense -c "Coinbase, Inc." -l "apache" -v
GO_PACKAGES=./asserter/... ./fetcher/... ./types/... ./client/... ./server/...
GO_FOLDERS=$(shell echo ${GO_PACKAGES} | sed -e "s/\.\///g" | sed -e "s/\/\.\.\.//g")
GO_INSTALL=GO111MODULE=off go get
TEST_SCRIPT=go test -v ${GO_PACKAGES}
LINT_SETTINGS=golint,misspell,gocyclo,gocritic,whitespace,goconst,gocognit,bodyclose,unconvert,lll,unparam

deps: | gen-deps
	go get ./...
	${GO_INSTALL} github.com/mattn/goveralls

gen-deps:
	${GO_INSTALL} github.com/google/addlicense
	${GO_INSTALL} github.com/segmentio/golines

gen:
	./codegen.sh

check-gen: | gen
	git diff --exit-code

lint-examples:
	cd examples; \
	golangci-lint run -v -E ${LINT_SETTINGS}

lint: | lint-examples
	golangci-lint run -v -E ${LINT_SETTINGS},gomnd

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
	golines -w --shorten-comments ${GO_FOLDERS} examples

shellcheck:
	shellcheck codegen.sh

salus:
	docker run --rm -t -v ${PWD}:/home/repo coinbase/salus

release: shellcheck check-gen check-license check-format test lint salus

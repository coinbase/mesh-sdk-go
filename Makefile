.PHONY: deps gen lint format check-format test test-coverage add-license \
	check-comments check-license shorten-lines shellcheck salus release mocks

# To run the the following packages as commands,
# it is necessary to use `go run <pkg>`. Running `go get` does
# not install any binaries that could be used to run
# the commands directly.
ADDLICENSE_INSTALL=go install github.com/google/addlicense@latest
ADDLICENSE_CMD=addlicense
ADDLICENCE_SCRIPT=${ADDLICENSE_CMD} -c "Coinbase, Inc." -l "apache" -v
GOIMPORTS_CMD=go run golang.org/x/tools/cmd/goimports
GOLINES_INSTALL=go install github.com/segmentio/golines@latest
GOLINES_CMD=golines
GOVERALLS_INSTALL=go install github.com/mattn/goveralls@latest
GOVERALLS_CMD=goveralls
GOLINT_CMD=go run golang.org/x/lint/golint
GO_PACKAGES=./asserter/... ./fetcher/... ./types/... ./client/... ./server/... \
	./parser/... ./syncer/... ./reconciler/... ./keys/... \
	./statefulsyncer/... ./storage/... ./utils/... ./constructor/... ./errors/...
GO_FOLDERS=$(shell echo ${GO_PACKAGES} | sed -e "s/\.\///g" | sed -e "s/\/\.\.\.//g")
TEST_SCRIPT=go test ${GO_PACKAGES}
LINT_SETTINGS=golint,misspell,gocyclo,gocritic,whitespace,goconst,gocognit,bodyclose,unconvert,lll,unparam

deps:
	go get ./...

gen:
	./codegen.sh;

check-gen: | gen
	git diff --exit-code

fix-imports:
	./imports.sh;

check-comments:
	${GOLINT_CMD} -set_exit_status ${GO_FOLDERS} .

lint-examples:
	cd examples; \
	golangci-lint run -v -E ${LINT_SETTINGS}

lint: | lint-examples
	golangci-lint run --timeout 2m0s -v -E ${LINT_SETTINGS},gomnd

format:
	gofmt -s -w -l .
	${GOIMPORTS_CMD} -w .

check-format:
	! gofmt -s -l . | read;
	! ${GOIMPORTS_CMD} -l . | read;

test:
	${TEST_SCRIPT}

test-cover:
	${GOVERALLS_INSTALL}
	if [ "${COVERALLS_TOKEN}" ]; then ${TEST_SCRIPT} -coverprofile=c.out -covermode=count; ${GOVERALLS_CMD} -coverprofile=c.out -repotoken ${COVERALLS_TOKEN}; fi

add-license:
	${ADDLICENSE_INSTALL}
	${ADDLICENCE_SCRIPT} .

check-license:
	${ADDLICENSE_INSTALL}
	${ADDLICENCE_SCRIPT} -check .

shorten-lines:
	${GOLINES_INSTALL}
	${GOLINES_CMD} -w --shorten-comments ${GO_FOLDERS} examples

shellcheck:
	shellcheck codegen.sh

salus:
	docker run --rm -t -v ${PWD}:/home/repo coinbase/salus

release: shellcheck check-gen check-license check-format test lint salus

mocks:
	rm -rf mocks;
	mockery --dir syncer --all --case underscore --outpkg syncer --output mocks/syncer;
	mockery --dir reconciler --all --case underscore --outpkg reconciler --output mocks/reconciler;
	mockery --dir constructor/worker --all --case underscore --outpkg worker --output mocks/constructor/worker;
	mockery --dir constructor/coordinator --all --case underscore --outpkg coordinator --output mocks/constructor/coordinator;
	mockery --dir utils --all --case underscore --outpkg utils --output mocks/utils;
	mockery --dir storage/database --all --case underscore --outpkg database --output mocks/storage/database;
	mockery --dir storage/modules --all --case underscore --outpkg modules --output mocks/storage/modules;

	${ADDLICENCE_SCRIPT} .;

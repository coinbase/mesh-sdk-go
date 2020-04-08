.PHONY: deps gen lint test add-license check-license circleci-local shellcheck salus shorten-lines
LICENCE_SCRIPT=addlicense -c "Coinbase, Inc." -l "apache" -v
GO_PACKAGES=./asserter/... ./fetcher/... ./gen/...

deps:
	go get ./...
	go get github.com/stretchr/testify
	go get github.com/davecgh/go-spew
	go get github.com/google/addlicense
	go get github.com/segmentio/golines

gen:
	./codegen.sh

lint:
	golangci-lint run -v \
		-E golint,misspell,gocyclo,gocritic,whitespace,goconst,gocognit,bodyclose,unconvert,lll,unparam,gomnd

format:
	gofmt -s -w -l .

check-format:
	! gofmt -s -l . | read

test:
	go test -v ${GO_PACKAGES} 

add-license:
	${LICENCE_SCRIPT} .

check-license:
	${LICENCE_SCRIPT} -check .

shorten-lines:
	golines -w --shorten-comments asserter fetcher gen 

shellcheck:
	shellcheck codegen.sh

salus:
	docker run --rm -t -v ${PWD}:/home/repo coinbase/salus

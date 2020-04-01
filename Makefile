.PHONY: deps gen lint test add-license check-license circleci-local shellcheck salus
LICENCE_SCRIPT=addlicense -c "Coinbase, Inc." -l "apache" -v

deps:
	go get ./...
	go get github.com/stretchr/testify
	go get github.com/davecgh/go-spew
	go get golang.org/x/lint/golint
	go get github.com/google/addlicense

gen:
	./codegen.sh

lint:
	golint -set_exit_status ./asserter/... ./fetcher/... ./gen/...

test:
	go test -v ./asserter ./fetcher

add-license:
	${LICENCE_SCRIPT} .

check-license:
	${LICENCE_SCRIPT} -check .

circleci-local:
	circleci local execute

shellcheck:
	shellcheck codegen.sh

salus:
	docker run --rm -t -v ${PWD}:/home/repo coinbase/salus

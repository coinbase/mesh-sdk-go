generate-code:
	./codegen.sh 

deps:
	go get ./...
	go get golang.org/x/lint/golint
	go get github.com/google/addlicense

lint:
	golint -set_exit_status ./asserter/... ./fetcher/...

test:
	go test -v ./asserter ./fetcher

LICENCE_SCRIPT=addlicense -c "Coinbase, Inc." -l "apache" -v

add-license:
	${LICENCE_SCRIPT} .

check-license:
	${LICENCE_SCRIPT} -check .

circleci-local:
	circleci local execute

salus:
	docker run --rm -t -v ${PWD}:/home/repo coinbase/salus

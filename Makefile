default: build test

all: test lint vulncheck

build: dirs
	go build -race -o bin ./...

test: dirs
	go test ./... -race -count=1 -coverprofile=bin/coverage.out

soaktest: dirs
	SOAK_CMD="make test" sh/soak.sh

int: FORCE
	GOLABELS=int go test -timeout 180s -v -race -count=1 ./int

soakint: FORCE
	SOAK_CMD="make int" sh/soak.sh

dirs:
	mkdir -p bin

lint:
	staticcheck -checks=all ./...

vulncheck:
	govulncheck ./...

clean:
	rm -rf bin

list: FORCE
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$'

FORCE:

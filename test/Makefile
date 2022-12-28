GOOS ?= linux
GOARCH ?= amd64
BIN_FILE=test

all: test

test:
	@mkdir -p bin
	GOOS=$(GOOS) GOARCH=$(GOARCH) go build -o ./bin/${BIN_FILE} test.go

clean:
	-rm -rf ./bin

.PHONY: all clean test

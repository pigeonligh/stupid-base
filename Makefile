BIN_DIR = _output/bin

LD_FLAGS=""

stupid-base: init
	go build -ldflags ${LD_FLAGS} -o ${BIN_DIR}/stupid-base ./cmd/stupid-base

init:
	mkdir -p ${BIN_DIR}

unit-test:
	go test -v -cover ./pkg/database/...

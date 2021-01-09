BIN_DIR=_output/bin
REPO_PATH=github.com/pigeonligh/stupid-base
GitSHA=`git rev-parse HEAD`
Date=`date "+%Y-%m-%d %H:%M:%S"`
REL_OSARCH="linux/amd64"
LD_FLAGS=" \
    -X '${REPO_PATH}/pkg/version.GitSHA=${GitSHA}' \
    -X '${REPO_PATH}/pkg/version.Built=${Date}'   \
    -X '${REPO_PATH}/pkg/version.Version=${RELEASE_VER}'"

stupid-base: init
	go build -v -ldflags ${LD_FLAGS} -o ${BIN_DIR}/stupid-base ./cmd/stupid-base

run: stupid-base
	${BIN_DIR}/stupid-base

init:
	mkdir -p ${BIN_DIR}

unit-test:
	go test -v -cover ./pkg/core/...

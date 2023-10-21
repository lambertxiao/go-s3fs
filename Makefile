export GO111MODULE=on

all:
GO := go
PKG := github.com/lambertxiao/go-s3fs/pkg/types
GO_S3FS_VERSION := $(shell git describe --tags --abbrev=0)
COMMIT_ID := $(shell git rev-parse --short HEAD)
GO_VERSION := $(shell $(GO) version|sed 's/go version[[:blank:]]//g')
BUILD_TIME := $(shell date +%Y-%m-%d:%H:%M:%S)
LDFLAGS =  # For macOS compatible processing
$(warning $(LDFLAGS))
LDFLAGS += -X $(PKG).GO_S3FS_VERSION=$(GO_S3FS_VERSION) \
	-X $(PKG).COMMIT_ID=$(COMMIT_ID) \
	-X '$(PKG).GO_VERSION=$(GO_VERSION)' \
	-X $(PKG).BUILD_TIME=$(BUILD_TIME)
ENV := $(shell $(GO) env)

env:
	$(warning $(ENV))

linux: env
	cd cmd && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -v -x -ldflags "${LDFLAGS}" -o ../go-s3fs.linux

macos:  env
	cd cmd && CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GO) build -v -x -ldflags "${LDFLAGS}" -o ../go-s3fs.macos

clean:
	rm -rf go-s3fs go-s3fs.exe

gen_mocks:
	find . -name "mocks_*" | xargs rm
	mockgen -source=pkg/storage/storage.go -destination pkg/mocks/mocks_storage.go -package=mocks
	mockgen -source=pkg/meta/meta.go -destination pkg/mocks/mocks_meta.go -package=mocks
	mockgen -source=pkg/common/clock.go -destination pkg/mocks/mocks_clock.go -package=mocks

run_test: gen_mocks
	go test -v ./...

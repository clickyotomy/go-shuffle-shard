GO111MODULE=on

fmt:
	go fmt ./...

test:
	go test ./...

build:
	go build ./...

vet:
	go vet ./...

mod:
	go mod download
	go mod tidy
	go mod verify 

dev: mod fmt vet build test

.PHONY: fmt test build vet mod dev

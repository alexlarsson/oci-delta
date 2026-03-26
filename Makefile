.PHONY: build clean test fmt

build:
	go build -o bootc-delta ./cmd/bootc-delta

clean:
	rm -f bootc-delta

test:
	go test ./...

fmt:
	go fmt ./...

install:
	go install ./cmd/bootc-delta

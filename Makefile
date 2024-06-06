build:
	go build -o node cmd/node/main.go

clean:
	rm -rf data-*

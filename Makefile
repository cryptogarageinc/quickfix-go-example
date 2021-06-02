SHELL := /bin/bash

test: lint vet build

test_win: lint vet build_win

lint:
	golint ./...

vet:
	go vet ./...

build: clean
	go build -v -o ./bin/publisher ./cmd/publisher
	go build -v -o ./bin/subscriber ./cmd/subscriber
	go build -v -o ./bin/wsserver ./cmd/websocket

build_win: clean_win
	go build -v -o ./bin/publisher.exe ./cmd/publisher
	go build -v -o ./bin/subscriber.exe ./cmd/subscriber
	go build -v -o ./bin/wsserver.exe ./cmd/websocket

clean:
	rm -rf ./bin ./tmp

clean_win:
	if exist bin ( rmdir /S /Q bin )
	if exist tmp ( rmdir /S /Q tmp )

exec_pub:
	./bin/publisher ./config/publisher.cfg

exec_sub:
	./bin/subscriber ./config/subscriber.cfg

exec_win: build_win
	CMD /C start CMD /K .\bin\publisher.exe .\config\publisher.cfg
	timeout 1
	CMD /C start CMD /K .\bin\subscriber.exe .\config\subscriber.cfg

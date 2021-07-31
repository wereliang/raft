all:
	go build -o raftexample main.go
	mkdir -p ./instance1/log; cp raftexample ./instance1; rm -rf ./instance1/data
	mkdir -p ./instance2/log; cp raftexample ./instance2; rm -rf ./instance2/data
	mkdir -p ./instance3/log; cp raftexample ./instance3; rm -rf ./instance3/data

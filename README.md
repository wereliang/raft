# Project Descption
This project implement raft by golang. It just for study and not for production, it just implement election and append log now.
It will be improved in some time.

# Build and run example
## Build
`cd ./cmd && make`
## Run example
```
./instance1/raftexample -id 1 -nodes 127.0.0.1:5566,127.0.0.1:5567,127.0.0.1:5568
./instance2/raftexample -id 2 -nodes 127.0.0.1:5566,127.0.0.1:5567,127.0.0.1:5568
./instance3/raftexample -id 3 -nodes 127.0.0.1:5566,127.0.0.1:5567,127.0.0.1:5568
```

- set key value to instance1 (if instance1 is leader):

`curl "http://127.0.0.1:5566/put?key=hello&value=world"`

- get key from instance2:

`curl "http://127.0.0.1:5567/get?key=hello"`

- get raft from any instance:

`curl "http://127.0.0.1:5566/state"`

- return redirect if not leader (if instance3 is not leader):

`curl "http://127.0.0.1:5568/put?key=hello&value=world"`




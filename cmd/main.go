package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/wereliang/raft/core"
	"github.com/wereliang/raft/pkg/xlog"
	"github.com/wereliang/raft/server"
)

type TestApp struct {
	data map[string]string
}

func (app *TestApp) Apply(data []byte) error {
	kv := strings.Split(string(data), "=")
	if len(kv) != 2 {
		fmt.Println("data invalid. ", string(data))
		return nil
	}
	app.data[kv[0]] = kv[1]
	return nil
}

func (app *TestApp) Query(key []byte) ([]byte, error) {
	if v, ok := app.data[string(key)]; ok {
		return []byte(v), nil
	}
	return nil, fmt.Errorf("not found key:%s", string(key))
}

func NewTestApp() *TestApp {
	return &TestApp{make(map[string]string)}
}

type Server struct {
	raft server.Instance
	app  *TestApp
	addr string
}

func (s *Server) Start() error {
	err := s.raft.Start()
	if err != nil {
		return err
	}

	http.HandleFunc("/put", LeaderCheck(s.handlePut, s.raft))
	// http.HandleFunc("/get", LeaderCheck(s.handleGet, s.raft))
	http.HandleFunc("/get", s.handleGet)
	http.HandleFunc("/state", s.handleState)
	go func() {
		http.ListenAndServe(s.addr, nil)
	}()
	return nil
}

func (s *Server) writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(err.Error()))
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	value := r.URL.Query()
	k := value.Get("key")
	v := value.Get("value")
	if k == "" || v == "" {
		s.writeError(w, fmt.Errorf("invalid key or value"))
		return
	}
	err := s.raft.AppendLog([]byte(fmt.Sprintf("%s=%s", k, v)))
	if err != nil {
		s.writeError(w, err)
		return
	}
	w.Write([]byte("success"))
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	value := r.URL.Query()
	k := value.Get("key")
	if k == "" {
		s.writeError(w, fmt.Errorf("invalid key"))
		return
	}
	v, err := s.app.Query([]byte(k))
	if err != nil {
		s.writeError(w, err)
		return
	}
	w.Write(v)
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	states := s.raft.QueryRaftState()
	data, _ := json.Marshal(states)
	w.Write(data)
}

func LeaderCheck(handler func(http.ResponseWriter, *http.Request), raft server.Instance) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if raft.Role() == core.Leader {
			handler(w, r)
		} else {
			leader, err := raft.Leader()
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}

			url := fmt.Sprintf("http://%s:%d", leader.Addr, leader.Port)
			http.Redirect(w, r, url, 302)
		}
	}
}

func NewServer(config *core.Config, app core.Application, nodes []*server.AppNode) (*Server, error) {
	raft, err := server.NewInstance(config, app, nodes)
	if err != nil {
		return nil, err
	}

	node := nodes[config.Node.ID-1]
	s := &Server{raft, app.(*TestApp), fmt.Sprintf("%s:%d", node.Addr, node.Port)}
	return s, nil
}

var (
	ID       int
	StrNodes string
)

// Two port type. One is raft rpc port, default is 9500. Here incr default for test multi instance
// Other is application port, the nodes mean application port.
func ParseConfig() (*core.Config, []*server.AppNode) {
	flag.IntVar(&ID, "id", -1, "local node id")
	flag.StringVar(&StrNodes, "nodes", "", "all nodes [include local]")
	flag.Parse()

	if ID < 1 {
		fmt.Println("please input correct id")
		return nil, nil
	}
	nodeArr := strings.Split(StrNodes, ",")
	if len(nodeArr) < 3 {
		fmt.Println("please input correct nodes. > 2")
		return nil, nil
	}

	var nodes []*core.Node
	var node *core.Node
	var appNodes []*server.AppNode

	for id, n := range nodeArr {
		ipport := strings.Split(n, ":")
		if len(ipport) != 2 {
			panic("invalid ip and port")
		}
		port, err := strconv.Atoi(ipport[1])
		if err != nil {
			panic(err)
		}

		// raft default http port
		newNode := &core.Node{Addr: ipport[0], Port: core.DefaultHTTPRPCPort + id, ID: int64(id + 1)}
		nodes = append(nodes, newNode)
		if int(newNode.ID) == ID {
			node = newNode
		}

		appNodes = append(appNodes, &server.AppNode{Addr: ipport[0], Port: port, ID: int64(id + 1)})
	}
	if node == nil {
		panic("ID and nodes not match")
	}

	return &core.Config{
		Node:          node,
		Nodes:         nodes,
		DataDir:       "./data",
		HeartBeatTime: 1000,
		VoteWaitTime:  3000,
		VoteRangeTime: 1000,
	}, appNodes
}

func GetCurrentDirectory() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}
	return strings.Replace(dir, "\\", "/", -1)
}

func main() {
	xlog.SetLogFilePath(GetCurrentDirectory()+"/log", "raft.log")

	config, nodes := ParseConfig()
	if config == nil {
		fmt.Println("config error")
		return
	}
	fmt.Printf("%#v\n", config)

	s, err := NewServer(config, &TestApp{make(map[string]string)}, nodes)
	if err != nil {
		panic(err)
	}
	err = s.Start()
	if err != nil {
		panic(err)
	}
	select {}
}

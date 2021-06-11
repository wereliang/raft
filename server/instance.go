package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wereliang/raft/core"
)

type AppNode struct {
	Addr string
	Port int
	ID   int64
}

type Instance interface {
	Start() error
	Stop() error
	AppendLog([]byte) error

	Role() core.Role
	Leader() (*AppNode, error)
	QueryRaftState() map[int64]*core.QueryStateResponse
	// TODO: add notify event, leader change
}

func NewInstance(config *core.Config, app core.Application, nodes []*AppNode) (Instance, error) {
	err := config.Check()
	if err != nil {
		return nil, err
	}

	transport := core.NewHTTPTransport()

	raft, err := core.NewRaft(config, transport, app)
	if err != nil {
		return nil, err
	}

	ins := &instanceImpl{
		raft:    raft,
		service: newRaftService(config, raft),
		nodes:   nodes,
	}

	return ins, nil
}

type instanceImpl struct {
	service RaftService
	raft    core.Raft
	nodes   []*AppNode
}

func (obj *instanceImpl) Start() error {
	obj.service.Start()
	obj.raft.Start()
	return nil
}

func (obj *instanceImpl) Stop() error {
	return nil
}

func (obj *instanceImpl) Role() core.Role {
	return obj.raft.State().Role
}

// TODO: cache, async or notify
func (obj *instanceImpl) Leader() (*AppNode, error) {
	var leaderID int64
	if obj.raft.State().Role == core.Leader {
		leaderID = obj.raft.Config().Node.ID
	}

	result := obj.QueryRaftState()
	for k, v := range result {
		if v != nil && v.State.Role == core.Leader {
			leaderID = k
			break
		}
	}

	if leaderID < 1 {
		return nil, fmt.Errorf("no leader!")
	}
	for _, node := range obj.nodes {
		if node.ID == leaderID {
			return node, nil
		}
	}
	panic("not found leader")
}

func (obj *instanceImpl) QueryRaftState() map[int64]*core.QueryStateResponse {
	result := make(map[int64]*core.QueryStateResponse)
	config := obj.raft.Config()
	result[config.Node.ID], _ = obj.raft.HandleQueryState(context.TODO())

	peers := obj.raft.Config().GetPeer()
	var wg sync.WaitGroup
	wg.Add(len(peers))
	var mutex sync.Mutex

	for _, p := range peers {
		go func(node *core.Node) {
			defer wg.Done()
			resp, err := obj.raft.Transport().State(node, time.Second*3)
			if err != nil {
				fmt.Printf("query state error. %v, %s", node, err)
			}
			mutex.Lock()
			result[node.ID] = resp
			mutex.Unlock()
		}(p.Node)
	}

	wg.Wait()
	return result
}

func (obj *instanceImpl) AppendLog(data []byte) error {
	return obj.raft.HandleAppAppend(context.TODO(), [][]byte{data})
}

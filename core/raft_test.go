package core

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wereliang/raft/log"
	"github.com/wereliang/raft/pkg/xlog"

	"github.com/stretchr/testify/assert"
)

var (
	config    *Config
	transport Transport
	raft      *raftImpl
	chanMap   map[int64]chanPair
	app       *TestApp
	// notifyC   chan *raftEvent
)

type action int

const (
	actNormal  action = iota // 正常响应，配合response
	actTimeout               // 响应超时
	actDown                  // peerdown掉
)

type RPCPair struct {
	request  interface{} // 期望的请求
	response interface{} // 返回的响应
}

type peerAction struct {
	id   int64
	act  action
	rpcs []*RPCPair
}

func mockPeerAction(acts []*peerAction, t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(len(acts))
	for i := 0; i < len(acts); i++ {
		go func(act *peerAction) {
			defer wg.Done()
			pair := getChanPair(act.id)
			for _, rpc := range act.rpcs {
				request := <-pair.input
				if rpc.request != nil {
					if expreq, ok := rpc.request.(*VoteRequest); ok {
						realreq := request.(*VoteRequest)
						assert.EqualValues(t, *expreq, *realreq)
					} else {
						expreq := rpc.request.(*AppendLogRequest)
						realreq := request.(*AppendLogRequest)
						assert.EqualValues(t, *expreq, *realreq)
					}
				}

				switch act.act {
				case actNormal:
					if rpc.response != nil {
						pair.output <- rpc.response
					}
				case actTimeout:
					// TODO
				case actDown:
				}
			}

		}(acts[i])
	}
	wg.Wait()
}

func getChanPair(id int64) chanPair {
	return chanMap[id]
}

type TestApp struct {
	data map[string]string
}

func (app *TestApp) Apply(data []byte) error {
	if app.data == nil {
		app.data = make(map[string]string)
	}

	kv := strings.Split(string(data), "=")
	if len(kv) != 2 {
		fmt.Println("invalid data.", string(data))
		return nil
	}
	app.data[kv[0]] = kv[1]
	return nil
}

func (app *TestApp) Query(key string) string {
	return app.data[key]
}

func (app *TestApp) Clear() {
	app.data = make(map[string]string)
}

func init() {
	xlog.SetLogFilePath("./log", "raft.log")

	app = &TestApp{}

	config = &Config{
		Node: &Node{"127.0.0.1", 9100, 1},
		Nodes: []*Node{
			{"127.0.0.1", 9100, 1},
			{"127.0.0.1", 9200, 2},
			{"127.0.0.1", 9300, 3},
			{"127.0.0.1", 9400, 4},
			{"127.0.0.1", 9500, 5},
		},
		HeartBeatTime: 1000,
		VoteWaitTime:  3000,
		VoteRangeTime: 1000,
	}
	chanMap = map[int64]chanPair{
		2: {make(chan interface{}), make(chan interface{})},
		3: {make(chan interface{}), make(chan interface{})},
		4: {make(chan interface{}), make(chan interface{})},
		5: {make(chan interface{}), make(chan interface{})},
	}
	transport = newChannelTransport(chanMap)
	var err error
	r, err := NewRaft(config, transport, app)
	if err != nil {
		panic(err)
	}
	raft = r.(*raftImpl)
	// notifyC = make(chan *raftEvent, 10)
	// raft.addNotify(notifyC)
}

func clearChann() {
	chanMap[2] = chanPair{make(chan interface{}), make(chan interface{})}
	chanMap[3] = chanPair{make(chan interface{}), make(chan interface{})}
	chanMap[4] = chanPair{make(chan interface{}), make(chan interface{})}
	chanMap[5] = chanPair{make(chan interface{}), make(chan interface{})}
}

func restartRaft(raft *raftImpl) {
	raft.Stop()
	raft.state.reset()
	raft.Start()

	clearChann()
	app.Clear()
}

func TestCandidate(t *testing.T) {

	raft.Start()
	state := raft.state
	peers := raft.getPeers()

	t.Run("init state", func(t *testing.T) {
		assert.Equal(t, state.Role, Follower)
		assert.Equal(t, state.CurrentTerm, int64(1))
		assert.Equal(t, state.VotedFor, int64(0))
		assert.Equal(t, len(peers), 4)
	})

	t.Run("投票不到半数，term+1持续选举；超时，周期内持续发送", func(t *testing.T) {
		mockPeerAction([]*peerAction{
			{2, actNormal, []*RPCPair{
				{&VoteRequest{2, 1, 0, 0}, &VoteResponse{2, true}},
				{&VoteRequest{3, 1, 0, 0}, &VoteResponse{3, true}},
				{&VoteRequest{4, 1, 0, 0}, &VoteResponse{4, true}},
			}},
			// 超时，周期内持续发送
			{3, actTimeout, []*RPCPair{
				{&VoteRequest{2, 1, 0, 0}, nil},
				{&VoteRequest{2, 1, 0, 0}, nil},
			}},
		}, t)
	})

	time.Sleep(time.Second * 1)
	restartRaft(raft)

	t.Run("返回term比自己大，变为follower", func(t *testing.T) {
		mockPeerAction([]*peerAction{
			{2, actNormal, []*RPCPair{
				{&VoteRequest{2, 1, 0, 0}, &VoteResponse{2, true}},
			}},
			// term 比自己大
			{3, actNormal, []*RPCPair{
				{&VoteRequest{2, 1, 0, 0}, &VoteResponse{10, false}},
			}},
		}, t)

		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, state.Role, Follower)
		assert.Equal(t, state.CurrentTerm, int64(10))
		assert.Equal(t, state.VotedFor, int64(0))
	})

	time.Sleep(time.Second * 1)
	restartRaft(raft)

	t.Run("正常投票，正常心跳", func(t *testing.T) {
		mockPeerAction([]*peerAction{
			{2, actNormal, []*RPCPair{
				{&VoteRequest{2, 1, 0, 0}, &VoteResponse{2, true}},
				{&AppendLogRequest{2, 1, 0, 0, nil, 0}, &AppendLogResponse{2, true, 0}},
			}},
			{3, actNormal, []*RPCPair{
				{&VoteRequest{2, 1, 0, 0}, &VoteResponse{2, true}},
				{&AppendLogRequest{2, 1, 0, 0, nil, 0}, &AppendLogResponse{2, true, 0}},
			}},
			// {4, actNormal, []*RPCPair{
			// 	{&VoteRequest{2, 1, 0, 0}, &VoteResponse{2, true}},
			// 	{&AppendLogRequest{2, 1, 0, 0, nil, 0}, &AppendLogResponse{2, true, 0}},
			// }},
			// {5, actNormal, []*RPCPair{
			// 	{&VoteRequest{2, 1, 0, 0}, &VoteResponse{2, true}},
			// 	{&AppendLogRequest{2, 1, 0, 0, nil, 0}, &AppendLogResponse{2, true, 0}},
			// }},
		}, t)

		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, state.Role, Leader)
		assert.Equal(t, state.CurrentTerm, int64(2))
		assert.Equal(t, state.VotedFor, int64(1))
	})

	time.Sleep(time.Second)
}

func TestAppAppend(t *testing.T) {

	// 返回term比自己大，变为follower
	go func() {
		err := raft.HandleAppAppend(nil, [][]byte{[]byte("hello=world")})
		if err != nil {
			fmt.Println("append app append error", err)
			return
		}
		fmt.Println("append success")
	}()

	time.Sleep(time.Second)

	t.Run("添加日志", func(t *testing.T) {
		mockPeerAction([]*peerAction{
			{2, actNormal, []*RPCPair{
				{&AppendLogRequest{2, 1, 0, 0, []*Entry{{2, 1, []byte("hello=world")}}, 0}, &AppendLogResponse{2, true, 1}},
			}},
			{3, actNormal, []*RPCPair{
				{&AppendLogRequest{2, 1, 0, 0, []*Entry{{2, 1, []byte("hello=world")}}, 0}, &AppendLogResponse{2, true, 1}},
			}},
		}, t)

		time.Sleep(time.Second)
		assert.Equal(t, app.Query("hello"), "world")
	})

	t.Run("对端节点超时，返回错误", func(t *testing.T) {
		fmt.Println(time.Now())
		err := raft.HandleAppAppend(nil, [][]byte{[]byte("name=xxx")})
		assert.NotNil(t, err)
		fmt.Printf("append fail. %s\n", err)
		fmt.Println(time.Now())
	})

}

func TestHandleVote(t *testing.T) {
	restartRaft(raft)

	state := raft.state
	// wait to vote term
	time.Sleep(10 * time.Second)
	assert.Equal(t, state.Role, Candidate)
	currterm := state.CurrentTerm
	fmt.Println("current term:", currterm)

	t.Run("收到term比自己小的vote请求，返回false", func(t *testing.T) {
		resp, err := raft.HandleVote(context.TODO(), &VoteRequest{currterm - 1, 2, 0, 0})
		assert.Nil(t, err)
		assert.Equal(t, resp.VoteGranted, false)
		assert.Equal(t, resp.Term, currterm)
	})

	t.Run("收到term和自己相同，但已经投票，返回false", func(t *testing.T) {
		resp, err := raft.HandleVote(context.TODO(), &VoteRequest{currterm, 2, 0, 0})
		assert.Nil(t, err)
		assert.Equal(t, resp.VoteGranted, false)
		assert.Equal(t, resp.Term, currterm)
	})

	// 之前的用例用添加日志了
	item := raft.raftLog.LastItem()
	t.Run("任期号比自己大，日志最新，投票给它，变为follower", func(t *testing.T) {
		currterm++
		resp, err := raft.HandleVote(context.TODO(), &VoteRequest{currterm, 2, item.Index, item.Term})
		assert.Nil(t, err)
		assert.Equal(t, resp.VoteGranted, true)
		assert.Equal(t, resp.Term, currterm)

		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, state.CurrentTerm, currterm)
		assert.Equal(t, state.VotedFor, int64(2))
		assert.Equal(t, state.Role, Follower)
	})

	// 让之前残留的消息消亡
	time.Sleep(time.Second * 3)

	// 成为leader
	t.Run("成为leader", func(t *testing.T) {
		mockPeerAction([]*peerAction{
			{2, actNormal, []*RPCPair{
				{&VoteRequest{currterm + 1, 1, item.Index, item.Index}, &VoteResponse{currterm + 1, true}},
			}},
			{3, actNormal, []*RPCPair{
				{&VoteRequest{currterm + 1, 1, item.Index, item.Index}, &VoteResponse{currterm + 1, true}},
			}},
		}, t)

		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, state.Role, Leader)
		assert.EqualValues(t, state.CurrentTerm, currterm+1)
		assert.EqualValues(t, state.VotedFor, 1)
	})

	t.Run("term 比自己大，日志非最新，返回false，更新term，变为follower", func(t *testing.T) {
		resp, err := raft.HandleVote(context.TODO(), &VoteRequest{10, 2, item.Index - 1, item.Term})
		assert.Nil(t, err)
		assert.Equal(t, resp.VoteGranted, false)
		// assert.Equal(t, resp.Term, 10)
		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, state.Role, Follower)
		assert.EqualValues(t, state.VotedFor, 0)
		assert.EqualValues(t, state.CurrentTerm, 10)
	})
}

func TestSyncLog(t *testing.T) {
	restartRaft(raft)
	state := raft.state

	// 设置5个item
	items := []*log.LogItem{
		{2, 0, []byte("a=1")},
		{2, 0, []byte("b=2")},
		{2, 0, []byte("c=3")},
		{2, 0, []byte("d=4")},
		{2, 0, []byte("e=5")},
	}
	lastIndex, _ := raft.raftLog.AppendLog(items, 1)
	assert.EqualValues(t, lastIndex, 5)
	lastItem := raft.raftLog.Index(lastIndex)

	t.Run("成为leader", func(t *testing.T) {
		mockPeerAction([]*peerAction{
			{2, actNormal, []*RPCPair{
				{&VoteRequest{2, 1, lastItem.Index, lastItem.Term}, &VoteResponse{lastItem.Index, true}},
			}},
			{3, actNormal, []*RPCPair{
				{&VoteRequest{2, 1, lastItem.Index, lastItem.Term}, &VoteResponse{lastItem.Index, true}},
			}},
		}, t)

		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, state.Role, Leader)
		assert.EqualValues(t, state.CurrentTerm, 2)
		assert.EqualValues(t, state.VotedFor, 1)
	})

	t.Run("同步所有日志", func(t *testing.T) {
		mockPeerAction([]*peerAction{
			{2, actNormal, []*RPCPair{
				{&AppendLogRequest{2, 1, 5, 2, nil, 0}, &AppendLogResponse{2, false, 0}},
				{&AppendLogRequest{2, 1, 4, 2, []*Entry{{2, 5, []byte("e=5")}}, 0}, &AppendLogResponse{2, false, 0}},
				{&AppendLogRequest{2, 1, 3, 2, []*Entry{{2, 4, []byte("d=4")}}, 0}, &AppendLogResponse{2, false, 0}},
				{&AppendLogRequest{2, 1, 2, 2, []*Entry{{2, 3, []byte("c=3")}}, 0}, &AppendLogResponse{2, false, 0}},
				{&AppendLogRequest{2, 1, 1, 2, []*Entry{{2, 2, []byte("b=2")}}, 0}, &AppendLogResponse{2, false, 0}},
				{&AppendLogRequest{2, 1, 0, 0, []*Entry{{2, 1, []byte("a=1")}}, 0}, &AppendLogResponse{2, true, 1}},

				{&AppendLogRequest{2, 1, 1, 2, []*Entry{{2, 2, []byte("b=2")}}, 0}, &AppendLogResponse{2, true, 2}},
				{&AppendLogRequest{2, 1, 2, 2, []*Entry{{2, 3, []byte("c=3")}}, 0}, &AppendLogResponse{2, true, 3}},
				{&AppendLogRequest{2, 1, 3, 2, []*Entry{{2, 4, []byte("d=4")}}, 0}, &AppendLogResponse{2, true, 4}},
				{&AppendLogRequest{2, 1, 4, 2, []*Entry{{2, 5, []byte("e=5")}}, 0}, &AppendLogResponse{2, true, 5}},

				{&AppendLogRequest{2, 1, 5, 2, nil, 0}, &AppendLogResponse{2, true, 5}},
			}},
		}, t)

		peers := raft.getPeers()
		for _, p := range peers {
			if p.ID == 2 {
				assert.EqualValues(t, p.MatchIndex, 5)
				assert.EqualValues(t, p.NextIndex, 6)
			}
		}
	})

	t.Run("同步多数并提交", func(t *testing.T) {
		mockPeerAction([]*peerAction{
			{3, actNormal, []*RPCPair{
				{&AppendLogRequest{2, 1, 5, 2, nil, 0}, &AppendLogResponse{2, false, 0}},
				{&AppendLogRequest{2, 1, 4, 2, []*Entry{{2, 5, []byte("e=5")}}, 0}, &AppendLogResponse{2, false, 0}},
				{&AppendLogRequest{2, 1, 3, 2, []*Entry{{2, 4, []byte("d=4")}}, 0}, &AppendLogResponse{2, false, 0}},
				{&AppendLogRequest{2, 1, 2, 2, []*Entry{{2, 3, []byte("c=3")}}, 0}, &AppendLogResponse{2, false, 0}},
				{&AppendLogRequest{2, 1, 1, 2, []*Entry{{2, 2, []byte("b=2")}}, 0}, &AppendLogResponse{2, false, 0}},
				{&AppendLogRequest{2, 1, 0, 0, []*Entry{{2, 1, []byte("a=1")}}, 0}, &AppendLogResponse{2, true, 1}},
				// 注意commitindex已经变更
				{&AppendLogRequest{2, 1, 1, 2, []*Entry{{2, 2, []byte("b=2")}}, 1}, &AppendLogResponse{2, true, 2}},
				{&AppendLogRequest{2, 1, 2, 2, []*Entry{{2, 3, []byte("c=3")}}, 2}, &AppendLogResponse{2, true, 3}},
				{&AppendLogRequest{2, 1, 3, 2, []*Entry{{2, 4, []byte("d=4")}}, 3}, &AppendLogResponse{2, true, 4}},
				{&AppendLogRequest{2, 1, 4, 2, []*Entry{{2, 5, []byte("e=5")}}, 4}, &AppendLogResponse{2, true, 5}},

				{&AppendLogRequest{2, 1, 5, 2, nil, 5}, &AppendLogResponse{2, true, 5}},
			}},
		}, t)
	})

	time.Sleep(time.Second)
	t.Run("数据已经提交应用", func(t *testing.T) {
		assert.Equal(t, app.Query("a"), "1")
		assert.Equal(t, app.Query("b"), "2")
		assert.Equal(t, app.Query("c"), "3")
		assert.Equal(t, app.Query("d"), "4")
		assert.Equal(t, app.Query("e"), "5")
	})
}

func TestHandleAppend(t *testing.T) {
	// 新增3条日志做测试
	items := []*log.LogItem{
		{2, 0, []byte("f=6")},
		{2, 0, []byte("g=7")},
	}
	lastIndex, _ := raft.raftLog.AppendLog(items)
	assert.EqualValues(t, lastIndex, 7)

	state := raft.state
	currTerm := state.CurrentTerm

	t.Run("term比自己小，返回false", func(t *testing.T) {
		resp, err := raft.HandleAppendLog(context.TODO(), &AppendLogRequest{Term: 1})
		assert.Nil(t, err)
		assert.Equal(t, resp.Success, false)
		assert.EqualValues(t, resp.Term, currTerm)
	})

	t.Run("term比自己大，切换follower，日志不匹配返回false", func(t *testing.T) {
		resp, err := raft.HandleAppendLog(context.TODO(), &AppendLogRequest{currTerm + 1, 2, 1, 1, nil, 0})
		assert.Nil(t, err)
		assert.Equal(t, resp.Success, false)
		assert.EqualValues(t, resp.Term, currTerm+1)
		assert.EqualValues(t, resp.MatchIndex, 0)
		time.Sleep(time.Millisecond * 100)
		assert.Equal(t, state.Role, Follower)
		assert.EqualValues(t, state.CurrentTerm, currTerm+1)
	})

	t.Run("日志匹配，有新的日志则更新替换，并提交", func(t *testing.T) {
		// 从第6个日志进行替换
		resp, err := raft.HandleAppendLog(
			context.TODO(),
			&AppendLogRequest{state.CurrentTerm, 2, 5, 2, []*Entry{{state.CurrentTerm, 0, []byte("f=666")}, {state.CurrentTerm, 0, []byte("g=777")}}, 6})
		assert.Nil(t, err)
		assert.Equal(t, resp.Success, true)
		assert.EqualValues(t, resp.MatchIndex, 7)
		time.Sleep(100 * time.Millisecond)

		lastItem := raft.raftLog.LastItem()
		assert.EqualValues(t, lastItem.Index, 7)
		assert.EqualValues(t, lastItem.Term, state.CurrentTerm)
		assert.EqualValues(t, lastItem.Data, []byte("g=777"))
		assert.EqualValues(t, state.CommitIndex, 6)
		assert.EqualValues(t, state.LastApplied, 6)
		assert.Equal(t, app.Query("f"), "666")
		assert.Equal(t, app.Query("g"), "") // 未提交
	})

}

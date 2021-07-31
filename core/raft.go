package core

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wereliang/raft/log"
	"github.com/wereliang/raft/pkg/xlog"
)

type Raft interface {
	Start() error
	Stop()
	Transport() Transport
	Config() *Config
	State() RaftState

	HandleVote(context.Context, *VoteRequest) (*VoteResponse, error)
	HandleAppendLog(context.Context, *AppendLogRequest) (*AppendLogResponse, error)
	HandleAppAppend(context.Context, [][]byte) error
	HandleQueryState(context.Context) (*QueryStateResponse, error)
}

type raftImpl struct {
	state     *RaftState
	config    *Config
	app       Application
	node      *Node
	peers     atomic.Value
	raftLog   log.RaftLog
	transport Transport

	appendC      chan *raftEvent // local -> raft -> processor
	notifyC      chan *raftEvent // processor-> raft
	notifyApplyC chan *raftEvent // processor-> raft -> application state

	done chan struct{}
	wg   sync.WaitGroup

	// processor locker, ensure process one event at onetime
	// protect processor switch when role change
	pmux  sync.RWMutex
	procs map[Role]Processor
	proce Processor
}

func NewRaft(config *Config, transport Transport, app Application) (Raft, error) {
	err := config.Check()
	if err != nil {
		return nil, err
	}
	peers := config.GetPeer()

	state, err := newRaftState(config.DataDir + "/state.json")
	if err != nil {
		return nil, err
	}

	// raftLog := log.NewMemLog()
	raftLog, err := log.NewStorage(config.DataDir)
	if err != nil {
		return nil, err
	}

	rand.Seed(time.Now().Unix())
	raft := &raftImpl{
		config:       config,
		state:        state,
		app:          app,
		node:         config.Node,
		raftLog:      raftLog,
		transport:    transport,
		appendC:      make(chan *raftEvent, 256),
		notifyC:      make(chan *raftEvent, 256),
		notifyApplyC: make(chan *raftEvent),
		done:         make(chan struct{}),
		procs:        make(map[Role]Processor),
	}
	raft.peers.Store(peers)

	comm := CommProcessor{
		config:    config,
		state:     state,
		log:       raft.raftLog,
		node:      config.Node,
		peer:      raft.peers,
		transport: transport,
		notifyC:   raft.notifyC,
	}

	raft.procs[Follower], _ = NewProcessor(Follower, comm)
	raft.procs[Candidate], _ = NewProcessor(Candidate, comm)
	raft.procs[Leader], _ = NewProcessor(Leader, comm)
	return raft, nil
}

func (r *raftImpl) become(role Role) bool {
	r.pmux.Lock()
	defer r.pmux.Unlock()
	if r.proce != nil && r.state.Role == role {
		xlog.Warn("switch same role:%d", role)
		return false
	}

	r.state.Role = role
	if r.proce != nil {
		// block wait
		r.proce.Stop()
	}
	proce := r.procs[role]
	go proce.Loop()
	r.proce = proce
	return true
}

func (r *raftImpl) getPeers() Peers {
	return r.peers.Load().(Peers)
}

func (r *raftImpl) raftLoop() {
	xlog.Debug("raft loop")
	r.done = make(chan struct{})
	r.become(Follower)

	r.wg.Add(3)
	defer r.wg.Done()
	go func() { defer r.wg.Done(); r.notifyLoop() }()
	go func() { defer r.wg.Done(); r.applyLoop() }()

LOOP:
	for {
		select {
		// TODO: batch append here
		// case appendC:
		case <-r.done:
			r.pmux.Lock()
			if r.proce != nil {
				r.proce.Stop()
				r.proce = nil
			}
			r.pmux.Unlock()
			break LOOP
		}
	}

	xlog.Debug("raft loop exit")
}

func (r *raftImpl) notifyLoop() {
LOOP:
	for {
		select {
		case evt := <-r.notifyC:
			switch evt.ename {
			case EventNotifySwitchRole:
				role := evt.data.(Role)
				xlog.Debug("%v", evt)
				r.become(role)
				// evt.resp <- true
			case EventNotifyApply:
				select {
				case r.notifyApplyC <- &raftEvent{}:
				default:
				}
			}
		case <-r.done:
			break LOOP
		}
	}
}

func (r *raftImpl) applyLoop() {
LOOP:
	for {
		select {
		case evt := <-r.notifyApplyC:
			xlog.Debug("apply notify %v", evt)
			r.apply()
		case <-r.done:
			break LOOP
		}
	}
}

func (r *raftImpl) apply() {
	var commitIndex, applyIndex int64
	r.state.RLock()
	commitIndex = r.state.CommitIndex
	applyIndex = r.state.LastApplied
	r.state.RUnlock()
	for applyIndex < commitIndex {
		// TODO: batch apply
		applyIndex++
		item := r.raftLog.Index(applyIndex)
		err := r.app.Apply(item.Data)
		if err != nil {
			xlog.Error("apply log fail")
			return
		}
		r.state.Lock()
		r.state.LastApplied = applyIndex
		r.state.Unlock()
	}
}

func (r *raftImpl) Start() error {
	go r.raftLoop()
	return nil
}

func (r *raftImpl) Stop() {
	close(r.done)
	r.wg.Wait()
}

func (r *raftImpl) State() RaftState {
	r.state.RLock()
	defer r.state.RUnlock()
	return r.state.Copy()
}

func (r *raftImpl) Transport() Transport {
	return r.transport
}

func (r *raftImpl) Config() *Config {
	return r.config
}

func (r *raftImpl) HandleVote(
	ctx context.Context,
	req *VoteRequest) (*VoteResponse, error) {
	r.pmux.RLock()
	defer r.pmux.RUnlock()
	if r.proce == nil {
		return nil, fmt.Errorf("internal error. processor is nil")
	}
	resp, err := r.proce.HandleEvent(
		newEvent(EventExtVoteRequest, req, nil))
	if err != nil {
		return nil, err
	}
	return resp.(*VoteResponse), nil
}

func (r *raftImpl) HandleAppendLog(
	ctx context.Context,
	req *AppendLogRequest) (*AppendLogResponse, error) {
	r.pmux.RLock()
	defer r.pmux.RUnlock()
	if r.proce == nil {
		return nil, fmt.Errorf("internal error. processor is nil")
	}
	resp, err := r.proce.HandleEvent(
		newEvent(EventExtAppendLogRequest, req, nil))
	if err != nil {
		return nil, err
	}
	return resp.(*AppendLogResponse), nil
}

func (r *raftImpl) HandleAppAppend(ctx context.Context, data [][]byte) error {
	// TODO: Performance bottleneck here!
	// Maybe send to channel to support batch append. And change with rwlock
	r.pmux.Lock()
	defer r.pmux.Unlock()
	if r.proce == nil {
		return fmt.Errorf("internal error. processor is nil")
	}

	resp, err := r.proce.HandleEvent(
		newEvent(EventAppAppendLogRequest, data, nil))
	if err != nil {
		return err
	}
	b := resp.(bool)
	if b {
		return nil
	}
	return fmt.Errorf("not commit by qurom")
}

func (r *raftImpl) HandleQueryState(context.Context) (*QueryStateResponse, error) {
	resp := &QueryStateResponse{}
	resp.State = r.State()
	peers := r.getPeers()
	for _, p := range peers {
		p.RLock()
		temp := p.Copy()
		p.RUnlock()
		resp.Peer = append(resp.Peer, &temp)
	}
	return resp, nil
}

package core

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/wereliang/raft/log"
	"github.com/wereliang/raft/pkg/xlog"
)

type DoneChan chan chan struct{}
type HandleFunc func(interface{}) (interface{}, error)

type Processor interface {
	// HandleEvent external event and application event
	HandleEvent(evt *raftEvent) (interface{}, error)
	Init() error
	Loop()
	Stop() bool
}

type CommProcessor struct {
	config    *Config
	state     *RaftState
	log       log.RaftLog
	node      *Node
	peer      atomic.Value
	transport Transport
	notifyC   chan *raftEvent
	done      DoneChan
	handlers  map[eventName]HandleFunc
	vtimer    *time.Timer // vote timer
}

func (p *CommProcessor) Init() error { return nil }
func (p *CommProcessor) Loop()       {}

func (p *CommProcessor) HandleEvent(evt *raftEvent) (interface{}, error) {
	fn, ok := p.handlers[evt.ename]
	if !ok {
		return nil, fmt.Errorf("not support event:%d", evt.ename)
	}
	return fn(evt.data)
}

func (p *CommProcessor) Stop() bool {
	// wait until stoped
	if p.done != nil {
		wait := make(chan struct{})
		p.done <- wait
		<-wait
		return true
	}
	return false
}

func (p *CommProcessor) getPeer() Peers {
	return p.peer.Load().(Peers)
}

func (p *CommProcessor) voteWait() time.Duration {
	return time.Duration((p.config.VoteWaitTime +
		rand.Intn(p.config.VoteRangeTime))) * time.Millisecond
}

func (p *CommProcessor) quorum() int {
	// TODO: when dynamic nodes
	return len(p.config.Nodes)/2 + 1
}

func (p *CommProcessor) notify(evt *raftEvent) interface{} {
	p.notifyC <- evt
	if evt.resc != nil {
		return <-evt.resc
	}
	return nil
}

func (p *CommProcessor) notifySwitch(role Role) {
	p.notify(newSwitchRoleEvent(role))
}

func (p *CommProcessor) handleVote(request interface{}) (interface{}, error) {
	req := request.(*VoteRequest)
	rsp := &VoteResponse{}
	p.state.Lock()
	defer p.state.Unlock()

	// term < local or == but has voted, return false
	if req.Term < p.state.CurrentTerm ||
		(req.Term == p.state.CurrentTerm && p.state.HasVoted()) {
		rsp.Term = p.state.CurrentTerm
		rsp.VoteGranted = false
		return rsp, nil
	}

	// others check log is newest
	if !p.isNewestLog(req.LastLogIndex, req.LastLogTerm) {
		rsp.Term = p.state.CurrentTerm
		rsp.VoteGranted = false
		// reset vote when term > local and has voted
		if req.Term > p.state.CurrentTerm && p.state.HasVoted() {
			p.state.ResetVote()
		}
		p.state.CurrentTerm = req.Term
	} else {
		p.state.VotedFor = req.CandidateID
		p.state.CurrentTerm = req.Term
		rsp.Term = req.Term
		rsp.VoteGranted = true
	}

	// if term > local , become follower
	if p.state.Role == Candidate || p.state.Role == Leader {
		p.notifySwitch(Follower)
	}
	return rsp, nil
}

// AppendLogResponse MatchIndex.
// -1 mean error, not match continue
// 0 mean not match, continue next time
// >0 mean math index
func (p *CommProcessor) handleAppend(request interface{}) (interface{}, error) {
	p.state.Lock()
	defer p.state.Unlock()

	req := request.(*AppendLogRequest)
	rsp := &AppendLogResponse{}

	xlog.Debug("request: %#v", req)
	defer xlog.Debug("response: %#v", rsp)

	if p.state.Role == Follower {
		p.vtimer.Reset(p.voteWait())
	}

	if req.Term < p.state.CurrentTerm {
		rsp.Term = p.state.CurrentTerm
		rsp.Success = false
		rsp.MatchIndex = -1
		return rsp, nil
	}

	if req.Term > p.state.CurrentTerm {
		p.state.CurrentTerm = req.Term
	}
	rsp.Term = p.state.CurrentTerm
	// when term > local, mean peer has quorum voted. local become follower
	if p.state.Role == Candidate || p.state.Role == Leader {
		p.notifySwitch(Follower)
		p.state.ResetVote()
	}

	// match prelog
	preItem := p.log.Index(req.PreLogIndex)
	if preItem.Index != req.PreLogIndex || preItem.Term != req.PreLogTerm {
		rsp.Success = false
		rsp.MatchIndex = 0
		return rsp, nil
	}

	// Append new log, here maybe cover the old log.
	// Are old commited log be covered? No, the leader must get newest log.
	if req.Entries != nil {
		items := make([]*log.LogItem, len(req.Entries))
		for i, entry := range req.Entries {
			items[i] = (*log.LogItem)(entry)
		}

		_, err := p.log.AppendLog(items, req.PreLogIndex+1)
		if err != nil {
			rsp.Success = false
			rsp.MatchIndex = -1
			return rsp, nil
		}
	}

	rsp.Success = true
	rsp.MatchIndex = preItem.Index + int64(len(req.Entries))

	// New commited index, notify to apply
	if req.LeaderCommit < p.state.CommitIndex {
		panic(fmt.Errorf("leader commit:%d < follower commit:%d",
			req.LeaderCommit, p.state.CommitIndex))
	}
	if req.LeaderCommit != p.state.CommitIndex {
		p.state.CommitIndex = req.LeaderCommit
		p.notify(newEvent(EventNotifyApply, p.state.CommitIndex, nil))
	}

	return rsp, nil
}

// Which log is newest? Compare last log's term, the bigger is newer.
// If last log's term is same, compare last index, the bigger is newer.
func (p *CommProcessor) isNewestLog(lastIndex, lastTerm int64) bool {
	localLast := p.log.LastItem()
	if localLast.Term > lastTerm {
		return false
	} else if localLast.Term == lastTerm {
		return localLast.Index <= lastIndex
	} else {
		return true
	}
}

type FollowerProcessor struct {
	CommProcessor
}

func (p *FollowerProcessor) Init() error {
	p.vtimer = NewStopTimer()
	p.handlers = map[eventName]HandleFunc{
		EventExtVoteRequest:      p.handleVote,
		EventExtAppendLogRequest: p.handleAppend,
	}
	return nil
}

func (p *FollowerProcessor) Loop() {
	xlog.Debug("follower loop")
	p.done = make(DoneChan)
	p.vtimer.Reset(p.voteWait())

LOOP:
	for {
		select {
		case <-p.vtimer.C:
			p.notifySwitch(Candidate)
		case x := <-p.done:
			p.vtimer.Stop()
			close(x)
			break LOOP
		}
	}
	xlog.Debug("follower stop")
}

///////////////////////////////////////

type CandidateProcessor struct {
	CommProcessor
	connMgr *PeerConnManager
}

func (p *CandidateProcessor) Init() error {
	p.connMgr = NewPeerConnMgr(p.getPeer(), p.transport)
	p.handlers = map[eventName]HandleFunc{
		EventExtVoteRequest:      p.handleVote,
		EventExtAppendLogRequest: p.handleAppend,
	}
	return nil
}

func (p *CandidateProcessor) initPeerGrant() {
	peers := p.getPeer()
	for _, peer := range peers {
		peer.Lock()
		peer.VoteGranted = false
		peer.Unlock()
	}
}

func (p *CandidateProcessor) voteLoop() *LoopCtl {
	p.initPeerGrant()

	logItem := p.log.LastItem()
	p.state.Lock()
	p.state.CurrentTerm += 1
	p.state.VotedFor = int64(p.node.ID)
	rpc := &rpcRequest{
		request: &VoteRequest{
			Term:         p.state.CurrentTerm,
			CandidateID:  p.node.ID,
			LastLogIndex: logItem.Index,
			LastLogTerm:  logItem.Term,
		},
		timeout: time.Second,
	}
	p.state.Unlock()

	var (
		granted = 1
		ctl     = NewLoopCtl()
		resc    = make(chan interface{}, 256)
		event   = newEvent(EventInnVoteRequest, rpc, resc)
	)

	xlog.Debug("begin vote: %v %v", event, rpc)

	ticker := time.NewTicker(time.Duration(p.config.VoteWaitTime/2) * time.Millisecond)
	var errConns []*PeerConnector // error node to retry

	go func() {
	LOOP:
		for {
			select {
			case res := <-resc:
				conn, b := p.doVoteResponse(res.(*raftEvent), event)
				if conn != nil {
					errConns = append(errConns, conn)
				} else if b {
					granted++
					if granted == p.quorum() {
						xlog.Debug("vote success. switch to leader")
						p.notifySwitch(Leader)
					}
				}
			case <-ticker.C:
				xlog.Debug("errConns:%d", len(errConns))
				if len(errConns) != 0 {
					for _, conn := range errConns {
						conn.SendDontWait(event)
					}
					errConns = nil
				}
			case <-ctl.C:
				xlog.Warn("exit vote loop")
				ticker.Stop()
				ctl.CloseResponse()
				break LOOP
			}
		}
	}()

	p.connMgr.BroadCast(event)
	return ctl
}

// state lock, mean deal one conn response one time
// return: exception connector. if response ok. return if granted
func (p *CandidateProcessor) doVoteResponse(response, request *raftEvent) (*PeerConnector, bool) {
	p.state.Lock()
	defer p.state.Unlock()

	rpcResp := response.GetRPCResponse()
	conn := rpcResp.object.(*PeerConnector)
	if rpcResp.err != nil {
		return conn, false
	}

	voteResponse := rpcResp.response.(*VoteResponse)
	if voteResponse.VoteGranted {
		conn.peer.VoteGranted = true
		xlog.Debug("vote success %v", conn.peer.Node)
		return nil, true
	}

	if voteResponse.Term > p.state.CurrentTerm {
		xlog.Debug("vote response term [%d] > current [%d]. switch to follower",
			voteResponse.Term, p.state.CurrentTerm)
		p.state.CurrentTerm = voteResponse.Term
		p.state.VotedFor = 0
		p.notifySwitch(Follower)
	}
	return nil, false
}

func (p *CandidateProcessor) Loop() {
	xlog.Debug("candiate loop")
	p.connMgr.Start()
	p.done = make(DoneChan)
	ctl := p.voteLoop()
	timer := time.NewTimer(p.voteWait())

LOOP:
	for {
		select {
		case <-timer.C:
			xlog.Warn("Vote time out, go to next term.")
			ctl.CloseAndRecv()
			ctl = p.voteLoop()
			timer.Reset(p.voteWait())
		case x := <-p.done:
			ctl.CloseAndRecv()
			p.connMgr.Close()
			close(x)
			break LOOP
		}
	}
	xlog.Debug("candidate stop")
}

///////////////////////////////

type DoAppendResponseFunc func(*raftEvent) int64

type LeaderProcessor struct {
	CommProcessor
	connMgr *PeerConnManager
	ctls    []*LoopCtl
	// applicaion trigger append log
	// sync channel wait util response
	syncC chan interface{}
}

func (p *LeaderProcessor) Init() error {
	peers := p.peer.Load().(Peers)
	p.connMgr = NewPeerConnMgr(peers, p.transport)
	p.syncC = make(chan interface{}, 256)

	p.handlers = map[eventName]HandleFunc{
		EventExtVoteRequest:      p.handleVote,
		EventExtAppendLogRequest: p.handleAppend,
		EventAppAppendLogRequest: p.handleAppAppend,
	}
	return nil
}

func (p *LeaderProcessor) handleAppAppend(request interface{}) (interface{}, error) {
	datas := request.([][]byte)
	items := make([]*log.LogItem, len(datas))
	for i, data := range datas {
		items[i] = &log.LogItem{Term: p.state.CurrentTerm, Data: data}
	}
	last, err := p.log.AppendLog(items)
	if err != nil {
		return false, fmt.Errorf("append log error. %s", err)
	}

	// TODO: If cann't reach quorum return directly, it also solve network split.
	for _, ctl := range p.ctls {
		select {
		case <-ctl.Send("append"):
		default:
			xlog.Warn("sync peer is busy")
		}
	}

	matchCnt := 1
	// TODO: timer define
	timer := time.NewTimer(time.Second * 5)
LOOP:
	for {
		select {
		case <-timer.C:
			break LOOP
		case res := <-p.syncC:
			// maybe consume last time's result
			// fn is routine local function doAppendResponse
			evt := res.(*raftEvent)
			fn := evt.GetRPCResponse().GetPeerConnector().argv.(func(evt *raftEvent) int64)
			if fn(evt) >= last {
				matchCnt++
				if matchCnt >= p.quorum() {
					break LOOP
				}
			}
		}
	}

	xlog.Debug("match:%d quorum:%d", matchCnt, p.quorum())
	if matchCnt >= p.quorum() {
		return true, nil
	}
	return false, nil
}

func (p *LeaderProcessor) resetTimer(timer *time.Timer) {
	timer.Reset(time.Duration(p.config.HeartBeatTime) * time.Millisecond)
}

func (p *LeaderProcessor) doAppendAndResetTimer(conn *PeerConnector, resc chan interface{}, block bool, timer *time.Timer) {
	p.state.RLock()
	defer p.state.RUnlock()
	p.doAppendRequest(conn, resc, block)
	p.resetTimer(timer)
}

// keepalive or append new log
func (p *LeaderProcessor) doAppendRequest(conn *PeerConnector, resc chan interface{}, block bool) {
	lastIndex := conn.peer.NextIndex - 1
	preLogItem := p.log.Index(lastIndex)

	// TODO: support batch append
	var entries []*Entry
	if lastIndex < p.log.LastIndex() {
		entries = append(entries, (*Entry)(p.log.Index(lastIndex+1)))
	}

	// request's commit param means max commit has sync to peer.
	var commit int64
	if p.state.CommitIndex > lastIndex {
		commit = lastIndex
	} else {
		commit = p.state.CommitIndex
	}

	rpc := &rpcRequest{
		request: &AppendLogRequest{
			Term:         p.state.CurrentTerm,
			LeaderID:     p.node.ID,
			PreLogIndex:  preLogItem.Index,
			PreLogTerm:   preLogItem.Term,
			Entries:      entries,
			LeaderCommit: commit,
		},
		timeout: time.Second,
	}

	event := newEvent(EventInnAppendLogRequest, rpc, resc)
	if block {
		conn.Send(event)
	} else {
		conn.SendDontWait(event)
	}
}

// doAppendResponse return match index
func (p *LeaderProcessor) doAppendResponse(event *raftEvent, resc chan interface{}, timer *time.Timer) int64 {
	p.state.Lock()
	defer p.state.Unlock()

	rpcResp := event.GetRPCResponse()
	if rpcResp.err != nil {
		xlog.Error("append response error. %s", rpcResp.err)
		return -1
	}

	var (
		response   = rpcResp.response.(*AppendLogResponse)
		conn       = rpcResp.object.(*PeerConnector)
		peer       = conn.peer
		matchIndex int64
	)

	matchIndex = response.MatchIndex
	if !response.Success {
		term := p.state.CurrentTerm
		if response.Term > term {
			p.state.CurrentTerm = response.Term
			p.state.VotedFor = 0
			xlog.Debug("leader switch to follower")
			p.notifySwitch(Follower)
			return matchIndex
		}
		// sync log
		if matchIndex == 0 {
			if peer.IsMatching() {
				peer.NextIndex--
			} else {
				// finish matching but return match:0
				// maybe follower restart but not serialize log,
				// and leader save last peer match data.
				// here just set peer match=0 and rematching
				peer.MatchIndex = 0
				if peer.NextIndex > 1 {
					peer.NextIndex--
				}
			}
		}
	} else {
		if matchIndex < 0 || matchIndex < peer.MatchIndex {
			panic("match index < 0 or < peer.matchIndex")
		}

		if peer.MatchIndex != matchIndex {
			peer.MatchIndex = matchIndex
		}
		peer.NextIndex = matchIndex + 1
		if peer.NextIndex > p.log.LastIndex()+1 {
			panic("next index error")
		}

		// check is quorum and then commit
		if matchIndex > p.state.CommitIndex {
			// leader self include
			commitCnt := 1
			peers := p.getPeer()
			for _, peer := range peers {
				if peer.MatchIndex >= matchIndex {
					commitCnt++
				}
				if commitCnt >= p.quorum() {
					p.state.CommitIndex = matchIndex
					// notify apply
					p.notify(newEvent(EventNotifyApply, p.state.CommitIndex, nil))
					break
				}
			}
		}
	}

	// continue sync if matching stage.
	// match=-1 mean error, keepalive next timer
	if matchIndex >= 0 {
		if peer.NextIndex != p.log.LastIndex()+1 {
			xlog.Debug("conn.peer.nextIndex != p.log.LastIndex()+1")
			// not call doAppendAndResetTimer here, will deadlock
			p.doAppendRequest(conn, resc, false)
			p.resetTimer(timer)
		}
	}
	return matchIndex
}

func (p *LeaderProcessor) initPeerMatch() {
	peers := p.getPeer()
	logItem := p.log.LastItem()
	for _, p := range peers {
		p.Lock()
		p.NextIndex = logItem.Index + 1
		p.MatchIndex = 0
		p.Unlock()
	}
}

func (p *LeaderProcessor) appendLoop() []*LoopCtl {
	p.initPeerMatch()
	conns := p.connMgr.Conns

	var ctls []*LoopCtl
	for _, conn := range conns {
		ctl := NewLoopCtl()
		ctls = append(ctls, ctl)

		go func(c *PeerConnector, ctl *LoopCtl) {
			resc := make(chan interface{}, 1)
			timer := NewStopTimer()

			// Application append log outside is sync, and will use resc and timer local routine.
			// So use closure function here
			c.argv = func(evt *raftEvent) int64 {
				return p.doAppendResponse(evt, resc, timer)
			}

			p.doAppendAndResetTimer(c, resc, false, timer)
		LOOP:
			for {
				select {
				case <-timer.C:
					xlog.Debug("heartbeat timeout")
					p.doAppendAndResetTimer(c, resc, false, timer)
				case res := <-resc:
					p.doAppendResponse(res.(*raftEvent), resc, timer)
				case cmd := <-ctl.C:
					switch cmd {
					case "append": // trigger by application and using syncC !!!
						p.doAppendAndResetTimer(c, p.syncC, true, timer)
						ctl.Response("")
					default:
						timer.Stop()
						ctl.CloseResponse()
						break LOOP
					}
				}
			}
		}(conn, ctl)
	}
	return ctls
}

func (p *LeaderProcessor) Loop() {
	xlog.Debug("leader loop")
	p.done = make(DoneChan)
	p.connMgr.Start()
	p.ctls = p.appendLoop()

LOOP:
	for {
		select {
		case x := <-p.done:
			xlog.Debug("leader recv done")
			// TODO: routine
			for _, ctl := range p.ctls {
				ctl.CloseAndRecv()
			}
			p.connMgr.Close()
			close(x)
			break LOOP
		}
	}
	xlog.Debug("leader exit")
}

func NewProcessor(role Role, comm CommProcessor) (Processor, error) {
	var proc Processor
	switch role {
	case Follower:
		proc = &FollowerProcessor{CommProcessor: comm}
	case Candidate:
		proc = &CandidateProcessor{CommProcessor: comm}
	case Leader:
		proc = &LeaderProcessor{CommProcessor: comm}
	default:
		return nil, fmt.Errorf("invalid processor type")
	}

	err := proc.Init()
	if err != nil {
		return nil, err
	}
	return proc, nil
}

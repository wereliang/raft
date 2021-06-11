package core

import (
	"sync"
	"sync/atomic"

	"github.com/wereliang/raft/pkg/xlog"
)

type Peers []*Peer

type Peer struct {
	sync.RWMutex
	*Node       `json:"node"`
	VoteGranted bool  `json:"granted"`
	NextIndex   int64 `json:"nextindex"`
	MatchIndex  int64 `json:"matchindex"`
}

func (p *Peer) Copy() Peer {
	return Peer{
		Node:        &Node{p.Addr, p.Port, p.ID},
		VoteGranted: p.VoteGranted,
		NextIndex:   p.NextIndex,
		MatchIndex:  p.MatchIndex,
	}
}

// IsMatching is log fall behind and matching
func (p *Peer) IsMatching() bool {
	return p.MatchIndex+1 < p.NextIndex
}

type PeerConnector struct {
	sync.RWMutex
	peer      *Peer
	transport Transport
	input     chan *raftEvent
	closed    bool
	argv      interface{} // custom param
}

func NewPeerConnector(peer *Peer, transport Transport) *PeerConnector {
	return &PeerConnector{
		peer:      peer,
		transport: transport,
		closed:    true,
	}
}

func (p *PeerConnector) Send(req *raftEvent) bool {
	p.RLock()
	defer p.RUnlock()
	if p.closed {
		return false
	}
	p.input <- req
	return true
}

func (p *PeerConnector) SendDontWait(req *raftEvent) bool {
	p.RLock()
	defer p.RUnlock()
	if p.closed {
		return false
	}

	select {
	case p.input <- req:
		return true
	default:
		return false
	}
}

func (p *PeerConnector) Peer() *Peer {
	return p.peer
}

func (p *PeerConnector) Start() {
	p.input = make(chan *raftEvent, 1)
	p.closed = false

	go func() {
		for evt := range p.input {
			switch evt.ename {
			case EventInnVoteRequest:
				p.vote(evt)
			case EventInnAppendLogRequest:
				p.appendLog(evt)
			}
		}
		xlog.Debug("connect close : %v", p.peer.Node)
	}()
}

func (p *PeerConnector) Stop() {
	p.Lock()
	defer p.Unlock()
	p.closed = true
	close(p.input)
}

func (p *PeerConnector) vote(evt *raftEvent) {
	rpcReq := evt.GetRPCRequest()
	voteReq := rpcReq.request.(*VoteRequest)
	xlog.Debug("[%#v] send vote:%#v", p.peer.Node, voteReq)
	voteRsp, err := p.transport.Vote(p.peer.Node, voteReq, rpcReq.timeout)
	if err != nil {
		xlog.Error("vote response error. %v %s", p.peer.Node, err)
	}
	evt.resc <- &raftEvent{
		data: &rpcResponse{voteRsp, err, p},
	}
	xlog.Debug("[%#v] recv vote:%#v", p.peer.Node, voteRsp)
}

func (p *PeerConnector) appendLog(evt *raftEvent) {
	rpcReq := evt.GetRPCRequest()
	appReq := rpcReq.request.(*AppendLogRequest)
	xlog.Debug("[%#v] send append:%#v", p.peer.Node, appReq)
	appRsp, err := p.transport.AppendLog(p.peer.Node, appReq, rpcReq.timeout)
	if err != nil {
		xlog.Error("append response error. %v %s", p.peer.Node, err)
	}
	evt.resc <- &raftEvent{
		data: &rpcResponse{appRsp, err, p},
	}
	xlog.Debug("[%#v] recv append:%#v", p.peer.Node, appRsp)
}

type ConnTag int8

const (
	// ConnAvail peer can append log normal
	ConnAvail ConnTag = iota
)

type PeerConnManager struct {
	Conns []*PeerConnector

	sync.RWMutex
	tagConns map[ConnTag]*atomic.Value
}

func (m *PeerConnManager) Start() {
	for _, conn := range m.Conns {
		conn.Start()
	}
}

func (m *PeerConnManager) Close() {
	for _, conn := range m.Conns {
		conn.Stop()
	}
	m.Lock()
	defer m.Unlock()
	m.tagConns = make(map[ConnTag]*atomic.Value)
}

func (m *PeerConnManager) BroadCast(event *raftEvent) {
	for _, conn := range m.Conns {
		if !conn.SendDontWait(event) {
			xlog.Debug("[%#v] send fail", conn.peer.Node)
		}
	}
}

func (m *PeerConnManager) AddTagConn(tag ConnTag, conn *PeerConnector) {
	m.Lock()
	defer m.Unlock()
	if val, ok := m.tagConns[tag]; ok {
		conns := val.Load().([]*PeerConnector)
		newConns := make([]*PeerConnector, 0)
		for _, c := range conns {
			if c.peer.ID != conn.peer.ID {
				newConns = append(newConns, c)
			}
		}
		newConns = append(newConns, conn)
		val.Store(newConns)
	} else {
		var val atomic.Value
		val.Store([]*PeerConnector{conn})
		m.tagConns[tag] = &val
	}
}

func (m *PeerConnManager) GetTagConn(tag ConnTag) []*PeerConnector {
	m.RLock()
	defer m.RUnlock()
	if val, ok := m.tagConns[tag]; ok {
		return val.Load().([]*PeerConnector)
	}
	return nil
}

func NewPeerConnMgr(peers Peers, transport Transport) *PeerConnManager {
	m := &PeerConnManager{}
	for _, p := range peers {
		m.Conns = append(m.Conns, NewPeerConnector(p, transport))
	}
	return m
}

package core

import (
	"fmt"

	"github.com/wereliang/raft/pkg/xlog"
)

const (
	RaftPort             = 9500
	DefaultHeartBeatTime = 50  // 50ms
	DefaultVoteWaitTime  = 200 // 200ms-300ms
	DefualtVoteRangeTime = 100
)

type Node struct {
	Addr string `json:"addr"`
	Port int    `json:"port"`
	ID   int64  `json:"id"`
}

func (n *Node) Check() error {
	if n.Port == 0 {
		return fmt.Errorf("node port zero")
	}
	if n.ID == 0 {
		return fmt.Errorf("invalid id")
	}
	return nil
}

type Config struct {
	Node          *Node   `json:"node"`  // local node
	Nodes         []*Node `json:"nodes"` // include local node
	DataDir       string  `json:"datadir"`
	HeartBeatTime int     `json:"heartbeattime"`
	VoteWaitTime  int     `json:"votewaittime"`
	VoteRangeTime int     `json:"voterangetime"`
}

func (c *Config) Check() error {
	if err := c.Node.Check(); err != nil {
		return fmt.Errorf("ID can't empty")
	}
	if len(c.Nodes) == 0 {
		return fmt.Errorf("peer can't nil")
	}
	if c.DataDir == "" {
		c.DataDir = "./data"
	}
	if c.HeartBeatTime == 0 {
		c.HeartBeatTime = DefaultHeartBeatTime
	}
	if c.VoteWaitTime == 0 {
		c.VoteWaitTime = DefaultVoteWaitTime
	}
	if c.VoteRangeTime == 0 {
		c.VoteRangeTime = DefualtVoteRangeTime
	}
	xlog.Debug("config:%v", c)
	return nil
}

// GetPeer return nodes exclude local node
func (c *Config) GetPeer() Peers {
	var peers Peers
	for _, n := range c.Nodes {
		if n.ID != c.Node.ID {
			peers = append(peers, &Peer{Node: n})
		}
	}
	return peers
}

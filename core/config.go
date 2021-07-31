package core

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/wereliang/raft/pkg/xlog"
)

const (
	RaftPort = 9500
	// DefaultHeartBeatTime = 50  // 50ms
	// DefaultVoteWaitTime  = 200 // 200ms-300ms
	// DefualtVoteRangeTime = 100
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

var defaultConfig = Config{
	HeartBeatTime: 50,  // 50ms
	VoteWaitTime:  200, // 200ms-300ms
	VoteRangeTime: 100,
}

func (c *Config) Check() error {
	if err := c.Node.Check(); err != nil {
		return fmt.Errorf("ID can't empty")
	}
	if len(c.Nodes) == 0 {
		return fmt.Errorf("peer can't nil")
	}

	if c.HeartBeatTime == 0 {
		c.HeartBeatTime = defaultConfig.HeartBeatTime
	}
	if c.VoteWaitTime == 0 {
		c.VoteWaitTime = defaultConfig.VoteWaitTime
	}
	if c.VoteRangeTime == 0 {
		c.VoteRangeTime = defaultConfig.VoteRangeTime
	}
	c.checkDataDir()
	xlog.Debug("config:%v", c)
	return nil
}

func (c *Config) checkDataDir() {
	if c.DataDir == "" {
		dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
		if err != nil {
			panic(err)
		}
		c.DataDir = filepath.Join(dir, "data")
	}

	err := os.MkdirAll(c.DataDir, os.ModePerm)
	if err != nil {
		panic(err)
	}
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

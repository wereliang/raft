package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/wereliang/raft/log"
)

type RPCName string

const (
	RPCVote   RPCName = "vote"
	RPCAppend RPCName = "append"
	RPCState  RPCName = "state"
)

type VoteRequest struct {
	Term         int64 `json:"term"`
	CandidateID  int64 `json:"candidateid"`
	LastLogIndex int64 `json:"lastlogindex"`
	LastLogTerm  int64 `json:"lastlogterm"`
}

type VoteResponse struct {
	Term        int64 `json:"term"`
	VoteGranted bool  `json:"votegranted"`
}

type Entry log.LogItem

type AppendLogRequest struct {
	Term         int64    `json:"term"`
	LeaderID     int64    `json:"leaderid"`
	PreLogIndex  int64    `json:"prelogindex"`
	PreLogTerm   int64    `json:"prelogterm"`
	Entries      []*Entry `json:"entries"`
	LeaderCommit int64    `json:"leadercommit"`
}

type AppendLogResponse struct {
	Term       int64 `json:"term"`
	Success    bool  `json:"success"`
	MatchIndex int64 `json:"matchindex"` // -1: error 0: not match  >0: match
}

type QueryStateResponse struct {
	State RaftState `json:"state"`
	Peer  Peers     `json:"peers"`
}

type Transport interface {
	Config() map[RPCName][]byte
	Vote(*Node, *VoteRequest, time.Duration) (*VoteResponse, error)
	AppendLog(*Node, *AppendLogRequest, time.Duration) (*AppendLogResponse, error)
	State(*Node, time.Duration) (*QueryStateResponse, error)
}

const (
	DefaultHTTPRPCPort = 9500
)

type httpTransport struct {
	// TODO: default http transport
	config  map[RPCName][]byte
	timeout time.Duration
}

func NewHTTPTransport() Transport {
	return &httpTransport{
		config: map[RPCName][]byte{
			RPCVote:   []byte("/vote"),
			RPCAppend: []byte("/append"),
			RPCState:  []byte("/state"),
		},
		timeout: 3 * time.Second,
	}
}

func (tp *httpTransport) Config() map[RPCName][]byte {
	return tp.config
}

func (tp *httpTransport) Vote(node *Node, voteReq *VoteRequest, timeout time.Duration) (*VoteResponse, error) {
	client := http.Client{Timeout: timeout}
	endpoint := fmt.Sprintf("http://%s:%d%s",
		node.Addr,
		node.Port,
		string(tp.config[RPCVote]))

	reqData, _ := json.Marshal(voteReq)
	request, _ := http.NewRequest("POST", endpoint, bytes.NewReader(reqData))
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()
	var voteResp VoteResponse
	rspData, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(rspData, &voteResp)
	if err != nil {
		return nil, err
	}
	return &voteResp, nil
}

func (tp *httpTransport) AppendLog(node *Node, appendReq *AppendLogRequest, timeout time.Duration) (*AppendLogResponse, error) {
	client := http.Client{Timeout: timeout}
	endpoint := fmt.Sprintf("http://%s:%d%s",
		node.Addr,
		node.Port,
		string(tp.config[RPCAppend]))

	reqData, _ := json.Marshal(appendReq)
	request, _ := http.NewRequest("POST", endpoint, bytes.NewReader(reqData))
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()
	var appendResp AppendLogResponse
	rspData, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(rspData, &appendResp)
	if err != nil {
		return nil, err
	}

	return &appendResp, nil
}

func (tp *httpTransport) State(node *Node, timeout time.Duration) (*QueryStateResponse, error) {
	client := http.Client{Timeout: timeout}
	endpoint := fmt.Sprintf("http://%s:%d%s",
		node.Addr,
		node.Port,
		string(tp.config[RPCState]))

	request, _ := http.NewRequest("POST", endpoint, nil)
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()
	var resp QueryStateResponse
	rspData, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(rspData, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

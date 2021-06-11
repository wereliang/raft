package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/wereliang/raft/core"
)

type RaftService interface {
	Start() error
	Stop(context.Context) error
}

func newRaftService(config *core.Config, raft core.Raft) RaftService {
	return &raftServiceImpl{
		config: config,
		raft:   raft,
	}
}

type raftServiceImpl struct {
	config *core.Config
	raft   core.Raft
	server *http.Server
}

func (svc *raftServiceImpl) Start() error {
	tpCfg := svc.raft.Transport().Config()

	handler := http.NewServeMux()
	handler.HandleFunc(string(tpCfg[core.RPCVote]), svc.HandleVote)
	handler.HandleFunc(string(tpCfg[core.RPCAppend]), svc.HandleAppendLog)
	handler.HandleFunc(string(tpCfg[core.RPCState]), svc.HandleQueryState)

	svc.server = &http.Server{
		Addr: fmt.Sprintf("%s:%d",
			svc.config.Node.Addr,
			svc.config.Node.Port),
		Handler: handler,
	}

	go func() {
		err := svc.server.ListenAndServe()
		if err != nil {
			panic(err)
		}
	}()
	return nil
}

func (svc *raftServiceImpl) Stop(ctx context.Context) error {
	return nil
}

func (svc *raftServiceImpl) handleError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(err.Error()))
}

func (svc *raftServiceImpl) HandleVote(w http.ResponseWriter, r *http.Request) {
	data, _ := ioutil.ReadAll(r.Body)
	var voteRequest core.VoteRequest
	err := json.Unmarshal(data, &voteRequest)
	if err != nil {
		svc.handleError(w, err)
		return
	}

	voteResponse, err := svc.raft.HandleVote(nil, &voteRequest)
	if err != nil {
		svc.handleError(w, err)
		return
	}
	resp, _ := json.Marshal(voteResponse)
	w.Write(resp)
	fmt.Println(string(resp))
}

func (svc *raftServiceImpl) HandleAppendLog(w http.ResponseWriter, r *http.Request) {
	data, _ := ioutil.ReadAll(r.Body)
	var appendRequest core.AppendLogRequest
	err := json.Unmarshal(data, &appendRequest)
	if err != nil {
		svc.handleError(w, err)
		return
	}

	appendResponse, err := svc.raft.HandleAppendLog(nil, &appendRequest)
	if err != nil {
		svc.handleError(w, err)
		return
	}
	resp, _ := json.Marshal(appendResponse)
	w.Write(resp)
}

func (svc *raftServiceImpl) HandleQueryState(w http.ResponseWriter, r *http.Request) {
	state, err := svc.raft.HandleQueryState(context.TODO())
	if err != nil {
		svc.handleError(w, err)
		return
	}
	resp, _ := json.Marshal(&state)
	w.Write(resp)
}

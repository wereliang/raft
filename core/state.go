package core

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"
)

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

type RaftState struct {
	sync.RWMutex
	// Persistence
	CurrentTerm int64 `json:"currentterm"`
	VotedFor    int64 `json:"votefor"`
	// Constantly changing
	CommitIndex int64 `json:"commitindex"`
	LastApplied int64 `json:"lastapplied"`
	// internal
	Role Role `json:"role"`
}

func (s *RaftState) Copy() RaftState {
	return RaftState{
		CurrentTerm: s.CurrentTerm,
		VotedFor:    s.VotedFor,
		CommitIndex: s.CommitIndex,
		LastApplied: s.LastApplied,
		Role:        s.Role,
	}
}

func (s *RaftState) LoadState(file string) error {
	_, err := os.Stat(file)
	if err == nil || os.IsExist(err) {
		data, err := ioutil.ReadFile(file)
		if err != nil {
			return err
		}
		statePersis := struct {
			CurrentTerm int64 `json:"currenttime"`
			VotedFor    int64 `json:"votefor"`
		}{}
		err = json.Unmarshal(data, &statePersis)
		if err != nil {
			return err
		}
		s.CurrentTerm = statePersis.CurrentTerm
		s.VotedFor = statePersis.VotedFor
	} else {
		s.CurrentTerm = 1
		s.VotedFor = 0
	}
	return nil
}

func (s *RaftState) HasVoted() bool {
	return s.VotedFor > 0
}

func (s *RaftState) ResetVote() {
	s.VotedFor = 0
}

func (s *RaftState) SaveState() {
}

// just for test
func (s *RaftState) reset() {
	s.Role = Follower
	s.CurrentTerm = 1
	s.VotedFor = 0
	s.CommitIndex = 0
	s.LastApplied = 0
}

func newRaftState(file string) (*RaftState, error) {
	state := &RaftState{
		Role: Follower,
	}
	err := state.LoadState(file)
	if err != nil {
		return nil, err
	}
	return state, nil
}

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
	// Persistence path
	FilePath string
}

type persistState struct {
	CurrentTerm int64 `json:"currentterm"`
	VotedFor    int64 `json:"votefor"`
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
		var tmpState persistState
		err = json.Unmarshal(data, &tmpState)
		if err != nil {
			return err
		}
		s.CurrentTerm = tmpState.CurrentTerm
		s.VotedFor = tmpState.VotedFor
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

// SaveState persist state to file
func (s *RaftState) SaveState() {
	tmpState := &persistState{s.CurrentTerm, s.VotedFor}
	f, err := os.OpenFile(s.FilePath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		panic("save state error." + err.Error())
	}
	defer f.Close()
	data, err := json.Marshal(tmpState)
	if err != nil {
		panic("save state error." + err.Error())
	}
	f.Write(data)
	f.Sync()
}

// Persist change current state and save to file
func (s *RaftState) Persist(term, vote int64) error {
	s.CurrentTerm, s.VotedFor = term, vote
	s.SaveState()
	return nil
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
		Role:     Follower,
		FilePath: file,
	}
	err := state.LoadState(file)
	if err != nil {
		return nil, err
	}
	return state, nil
}

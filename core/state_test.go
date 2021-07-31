package core

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatePersist(t *testing.T) {
	file := "./test_state.json"
	state, err := newRaftState(file)
	assert.Nil(t, err)
	assert.EqualValues(t, state.CurrentTerm, 1)
	assert.EqualValues(t, state.VotedFor, 0)

	state.CurrentTerm = 100
	state.VotedFor = 3
	state.SaveState()

	state2, err := newRaftState(file)
	assert.Nil(t, err)
	assert.EqualValues(t, state2.CurrentTerm, 100)
	assert.EqualValues(t, state2.VotedFor, 3)

	os.Remove(file)
}

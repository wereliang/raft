package core

// Application for state machine
type Application interface {
	Apply([]byte) error
}

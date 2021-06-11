package core

import "time"

type eventName int8

// Event appointment
// Inn: Innernal event, by local, include candidate vote, leader append ...
// Ext: External event, by peer, inlcude vote, append ...
// App: Application event, by application, include append log ...
// Notify: Notify event, include state machine, role switch
const (
	EventInnVoteRequest eventName = iota
	EventInnAppendLogRequest
	EventExtVoteRequest
	EventExtAppendLogRequest
	EventAppAppendLogRequest
	EvnetAppPeerChange
	EventNotifySwitchRole
	EventNotifyApply
)

type raftEvent struct {
	ename eventName
	data  interface{}
	resc  chan interface{}
}

func (evt *raftEvent) GetRPCRequest() *rpcRequest {
	return evt.data.(*rpcRequest)
}
func (evt *raftEvent) GetRPCResponse() *rpcResponse {
	return evt.data.(*rpcResponse)
}

type rpcRequest struct {
	request interface{}
	timeout time.Duration
}

type rpcResponse struct {
	response interface{}
	err      error
	object   interface{} // extend object
}

func (rpc *rpcResponse) GetPeerConnector() *PeerConnector {
	return rpc.object.(*PeerConnector)
}

func newEvent(name eventName, data interface{}, resc chan interface{}) *raftEvent {
	return &raftEvent{ename: name, data: data, resc: resc}
}

func newSwitchRoleEvent(role Role) *raftEvent {
	return newEvent(EventNotifySwitchRole, role, nil)
}

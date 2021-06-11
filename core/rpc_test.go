package core

import (
	"fmt"
	"time"

	"git.yingzhongtong.com/combase/gowork/src/xiaoying/common/xylog"
)

type chanPair struct {
	input  chan interface{}
	output chan interface{}
}

type channelTransport struct {
	chans map[int64]chanPair
}

func newChannelTransport(chans map[int64]chanPair) Transport {
	return &channelTransport{chans}
}

func (tp *channelTransport) Config() map[RPCName][]byte {
	return nil
}

func (tp *channelTransport) Vote(node *Node, req *VoteRequest, timeout time.Duration) (*VoteResponse, error) {
	if pair, ok := tp.chans[node.ID]; !ok {
		return nil, fmt.Errorf("not found channel pair for node:%d", node.ID)
	} else {
		timer := time.NewTimer(timeout)
		select {
		case pair.input <- req:
			timer.Reset(timeout)
		case <-timer.C:
			xylog.Debug("channel input timeout. node:%d", node.ID)
			return nil, fmt.Errorf("channel input timeout")
		}

		select {
		case <-timer.C:
			xylog.Debug("channel output timeout. %v", node)
			return nil, fmt.Errorf("channel output timeout")
		case resp := <-pair.output:
			timer.Stop()
			return resp.(*VoteResponse), nil
		}
	}
}

func (tp *channelTransport) AppendLog(node *Node, req *AppendLogRequest, timeout time.Duration) (*AppendLogResponse, error) {
	if pair, ok := tp.chans[node.ID]; !ok {
		return nil, fmt.Errorf("not found channel pair for node:%d", node.ID)
	} else {
		// pair.input <- req
		timer := time.NewTimer(timeout)

		select {
		case pair.input <- req:
			timer.Reset(timeout)
		case <-timer.C:
			xylog.Debug("channel input timeout. %v", node)
			return nil, fmt.Errorf("channel input timeout")
		}

		select {
		case <-timer.C:
			xylog.Debug("channel output timeout. %v", node)
			return nil, fmt.Errorf("channel output timeout")
		case resp := <-pair.output:
			timer.Stop()
			return resp.(*AppendLogResponse), nil
		}
	}
}

func (tp *channelTransport) State(*Node, time.Duration) (*QueryStateResponse, error) {
	return nil, nil
}

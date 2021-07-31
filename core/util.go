package core

import (
	"os"
	"time"
)

func NewStopTimer() *time.Timer {
	timer := time.NewTimer(10 * time.Second)
	timer.Stop()
	return timer
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

type LoopCtl struct {
	C chan string
	R chan interface{}
}

func (ctl *LoopCtl) Send(cmd string) chan interface{} {
	ctl.C <- cmd
	return ctl.R
}

func (ctl *LoopCtl) SendAndRecv(cmd string) interface{} {
	ctl.C <- cmd
	return <-ctl.R
}

func (ctl *LoopCtl) CloseAndRecv() interface{} {
	close(ctl.C)
	return <-ctl.R
}

func (ctl *LoopCtl) Response(data interface{}) {
	ctl.R <- data
}

func (ctl *LoopCtl) CloseResponse() {
	close(ctl.R)
}

func NewLoopCtl() *LoopCtl {
	return &LoopCtl{C: make(chan string, 1), R: make(chan interface{}, 1)}
}

package wal

import "errors"

var (
	ErrCorrupt    = errors.New("wal corrupt")
	ErrOutOfOrder = errors.New("out of order")
)

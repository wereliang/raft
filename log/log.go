package log

var (
	NilLogItem = &LogItem{0, 0, nil}
)

type LogItem struct {
	Term  int64  `json:"term"`
	Index int64  `json:"index"`
	Data  []byte `json:"data"`
}

// RaftLog index mean log index ID(start from 1), not array position.
type RaftLog interface {
	// AppendLog return last index
	AppendLog([]*LogItem, ...int64) (int64, error)

	Index(int64) *LogItem

	// Last log index
	LastIndex() int64

	LastItem() *LogItem

	// just test
	Dump()
}

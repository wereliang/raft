package log

import (
	"fmt"
	"sync"
)

// indexID start from 1
type memLog struct {
	sync.RWMutex
	logs  []*LogItem
	first int64 // start index
	last  int64 // last index
}

func NewMemLog() RaftLog {
	// maybe init first and last index
	return &memLog{}
}

func (l *memLog) AppendLog(items []*LogItem, index ...int64) (int64, error) {
	l.Lock()
	defer l.Unlock()

	if len(items) == 0 {
		return l.last, nil
	}

	if len(index) > 0 {
		newIndex := index[0]
		if newIndex < 1 || newIndex > l.last+1 {
			return l.last, fmt.Errorf("invalid index(%d) last(%d)",
				newIndex, l.last)
		}
		for i, item := range items {
			item.Index = newIndex + int64(i)
		}
		l.logs = append(l.logs[:newIndex-1], items...)
		l.last = items[len(items)-1].Index
		return l.last, nil
	}

	for i, item := range items {
		item.Index = l.last + int64(i) + 1
	}
	l.logs = append(l.logs, items...)
	l.last = items[len(items)-1].Index
	return l.last, nil
}

func (l *memLog) Index(index int64) *LogItem {
	l.RLock()
	defer l.RUnlock()
	if index < 1 || index > l.last {
		return NilLogItem
	}
	return l.logs[index-1]
}

func (l *memLog) LastIndex() int64 {
	l.RLock()
	defer l.RUnlock()
	return l.last
}

func (l *memLog) Dump() {
	l.RLock()
	defer l.RUnlock()
	fmt.Printf("last:%d ", l.last)
	for _, l := range l.logs {
		fmt.Printf("%#v ", *l)
	}
	fmt.Println()
}

func (l *memLog) LastItem() *LogItem {
	l.RLock()
	defer l.RUnlock()
	if l.last == 0 {
		return NilLogItem
	}
	return l.logs[l.last-1]
}

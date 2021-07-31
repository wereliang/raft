package log

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/wereliang/raft/pkg/wal"
)

type storage struct {
	sync.RWMutex
	wal *wal.Wal
}

func NewStorage(path string) (RaftLog, error) {
	w, err := wal.NewWal(path, &wal.Option{
		SegmentNum: 4,
		// SegmentSize: 20 * 1024 * 1024,
		SegmentSize: 20 * 1024,
		IsSync:      true})
	if err != nil {
		return nil, err
	}
	return &storage{wal: w}, nil
}

func (s *storage) AppendLog(items []*LogItem, index ...int64) (int64, error) {
	s.Lock()
	defer s.Unlock()

	first, last := s.wal.First(), s.wal.Last()
	if len(items) == 0 {
		return last, nil
	}

	start := last + 1
	var entries wal.Entries
	if len(index) > 0 {
		newIndex := index[0]
		if newIndex < first || newIndex > last+1 {
			return last, fmt.Errorf("invalid index(%d) last(%d)", newIndex, last)
		}
		if newIndex != last+1 {
			fmt.Printf("newIndex:%d last:%d\n", newIndex, last)
			if err := s.wal.TruncateBack(newIndex - 1); err != nil {
				return last, fmt.Errorf("truncate error. %s", err)
			}
		}
		start = newIndex
	}

	for i, item := range items {
		item.Index = int64(i) + start
		// data, err := json.Marshal(item)
		data, err := s.MarshalEntry(item)
		if err != nil {
			return last, err
		}
		entries = append(entries, wal.Entry{Index: item.Index, Data: data})
	}

	err := s.wal.AppendBatch(entries)
	if err != nil {
		return last, err
	}
	return entries[len(entries)-1].Index, nil
}

func (s *storage) MarshalEntry(item *LogItem) ([]byte, error) {
	var data []byte
	var buf [10]byte
	n := binary.PutUvarint(buf[:], uint64(item.Term))
	data = append(data, buf[:n]...)
	data = append(data, item.Data...)
	return data, nil
}

func (s *storage) UnmarshalEntry(data []byte, item *LogItem) error {
	term, n := binary.Uvarint(data)
	if n <= 0 {
		return fmt.Errorf("ErrCorrupt")
	}
	item.Term = int64(term)
	item.Data = data[n:]
	return nil
}

func (s *storage) Index(index int64) *LogItem {
	s.RLock()
	defer s.RUnlock()
	data, err := s.wal.Get(index)
	if err != nil {
		return NilLogItem
	}
	var item LogItem
	// if err = json.Unmarshal(data, &item); err != nil {
	// 	return NilLogItem
	// }
	if err = s.UnmarshalEntry(data, &item); err != nil {
		return NilLogItem
	}
	item.Index = index
	return &item
}

func (s *storage) LastIndex() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.wal.Last()
}

func (s *storage) LastItem() *LogItem {
	s.RLock()
	defer s.RUnlock()
	last := s.wal.Last()
	data, err := s.wal.Get(last)
	if err != nil {
		return NilLogItem
	}
	var item LogItem
	// if err = json.Unmarshal(data, &item); err != nil {
	// 	return NilLogItem
	// }
	if err = s.UnmarshalEntry(data, &item); err != nil {
		return NilLogItem
	}
	item.Index = last
	return &item
}

func (s *storage) Dump() {
	for i := s.wal.First(); i <= s.wal.Last(); i++ {
		s.wal.Get(i)
	}
}

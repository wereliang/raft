package wal

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
)

const SegMeta = ".SM."

// RecoverSegment recover segment from exist file
func RecoverSegment(name, path string) (*Segment, error) {
	if len(name) < 20 {
		return nil, fmt.Errorf("invalid segment name: %s", name)
	}
	index, err := strconv.ParseInt(name[:20], 10, 64)
	if err != nil || index < 1 {
		return nil, fmt.Errorf("invalid segment name: %s", name)
	}

	spath := filepath.Join(path, name)
	file, err := os.OpenFile(spath, os.O_RDONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("open segment file error: %s", err)
	}
	buf := make([]byte, 4)
	n, err := file.Read(buf)
	if err != nil || n != 4 {
		return nil, fmt.Errorf("read file error: %s", err)
	}
	if string(buf) != SegMeta {
		return nil, fmt.Errorf("invalid segment meta")
	}

	return &Segment{first: index, path: spath}, nil
}

// CreateSegment create segment by index and path
func CreateSegment(idx int64, path string) (*Segment, error) {
	spath := filepath.Join(path, segmentName(idx))
	file, err := os.Create(spath)
	if err != nil {
		return nil, err
	}
	n, err := file.Write([]byte(SegMeta))
	if err != nil || n != 4 {
		return nil, fmt.Errorf("write segment meta error")
	}
	return &Segment{
		first: idx,
		path:  spath,
		file:  file,
		last:  idx - 1,
	}, nil
}

func segmentName(idx int64) string {
	return fmt.Sprintf("%020d", idx)
}

type Segment struct {
	first int64 // first index, static
	last  int64 // last index, dymanic
	path  string
	items []*segItem
	data  []byte
	file  *os.File
}

type segItem struct {
	pos uint64
	end uint64
}

func (s *Segment) Load() error {
	file, err := os.OpenFile(s.path, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	s.file = file

	data = data[4:]
	temp := data
	var items []*segItem
	var pos uint64
	for len(data) > 0 {
		n, err := loadNextBinaryEntry(data)
		if err != nil {
			return err
		}
		data = data[n:]
		items = append(items, &segItem{pos, pos + uint64(n)})
		pos += uint64(n)
	}

	s.data = temp
	s.items = items
	s.last = s.first + int64(len(s.items)-1)
	return nil
}

func (s *Segment) Write(datas [][]byte, sync bool) error {
	oldPos, pos := uint64(len(s.data)), uint64(len(s.data))
	var nvar int

	for _, data := range datas {
		s.data, nvar = appendUvarint(s.data, uint64(len(data)))
		s.items = append(s.items, &segItem{pos, pos + uint64(nvar+len(data))})
		s.data = append(s.data, data...)
		pos = pos + uint64(nvar+len(data))
	}

	_, err := s.file.Write(s.data[oldPos:])
	if err != nil {
		// rollback
		s.items = s.items[:len(s.items)-len(datas)]
		s.data = s.data[:oldPos]
		return fmt.Errorf("write file error. %s", err)
	}

	s.last += int64(len(datas))

	if sync {
		err = s.file.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Segment) Read(idx int64) ([]byte, error) {
	pos := idx - s.first
	if pos < 0 {
		return nil, ErrOutOfOrder
	}
	item := s.items[pos]
	data := s.data[item.pos:item.end]
	size, n := binary.Uvarint(data)
	return data[n : uint64(n)+size], nil
}

func (s *Segment) Length() int {
	return len(s.data)
}

func (s *Segment) First() int64 {
	return s.first
}

func (s *Segment) Last() int64 {
	return s.last
}

func (s *Segment) Close() {
	if s.file != nil {
		s.file.Close()
	}
	s.items = nil
	s.data = nil
}

func appendUvarint(dst []byte, x uint64) ([]byte, int) {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], x)
	dst = append(dst, buf[:n]...)
	return dst, n
}

// return len size + data len
func loadNextBinaryEntry(data []byte) (int, error) {
	// data_size + data
	size, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, ErrCorrupt
	}
	if uint64(len(data)-n) < size {
		return 0, ErrCorrupt
	}
	return n + int(size), nil
}

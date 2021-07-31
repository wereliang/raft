package wal

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

type Option struct {
	SegmentNum  int  // the num segment in cache
	SegmentSize int  // the segment file size
	IsSync      bool // if flush sync after append
}

var defaultOption = Option{
	SegmentNum:  2,
	SegmentSize: 20 * 1024 * 1024,
	IsSync:      false,
}

type Wal struct {
	sync.RWMutex
	opts      *Option
	segs      []*Segment
	path      string
	first     int64 // first index for all segment
	cacheLock sync.Mutex
	cache     *Cache
}

type Entries []Entry

type Entry struct {
	Index int64
	Data  []byte
}

func NewWal(path string, opts *Option) (*Wal, error) {
	if opts == nil {
		opts = &defaultOption
	} else {
		if opts.SegmentNum <= 0 {
			opts.SegmentNum = defaultOption.SegmentNum
		}
		if opts.SegmentSize <= 0 {
			opts.SegmentSize = defaultOption.SegmentSize
		}
	}

	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}

	finfos, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var segs []*Segment
	// recover segment
	startIdx, endIdx, clearIdx := -1, -1, -1
	for _, info := range finfos {
		name := info.Name()
		if info.IsDir() {
			continue
		}

		seg, err := RecoverSegment(name, path)
		if err != nil {
			continue
		}
		segs = append(segs, seg)
		if len(name) == 26 && strings.HasSuffix(name, ".START") {
			startIdx = len(segs) - 1
		}
		if len(name) == 24 && strings.HasSuffix(name, ".END") {
			endIdx = len(segs) - 1
		}
		if len(name) == 26 && strings.HasSuffix(name, ".CLEAR") {
			clearIdx = len(segs) - 1
		}
	}
	// remove if truncating
	if startIdx != -1 {
		for i := 0; i < startIdx; i++ {
			if err = os.Remove(segs[i].path); err != nil {
				return nil, err
			}
		}
		segs = append([]*Segment{}, segs[startIdx:]...)
		oldPath := segs[0].path
		newPath := oldPath[:len(oldPath)-len(".START")]
		if err = os.Rename(oldPath, newPath); err != nil {
			return nil, err
		}
		segs[0].path = newPath
	}

	if endIdx != -1 {
		for i := endIdx + 1; i < len(segs); i++ {
			if err = os.Remove(segs[i].path); err != nil {
				return nil, err
			}
		}

		oldPath := segs[endIdx].path
		newPath := oldPath[:len(oldPath)-len(".END")]
		if err = os.Rename(oldPath, newPath); err != nil {
			return nil, err
		}
		segs = append([]*Segment{}, segs[:endIdx]...)
	}

	if clearIdx != -1 {
		for i, seg := range segs {
			if i != clearIdx {
				err = os.Remove(seg.path)
				if err != nil {
					return nil, err
				}
			}
		}

		clearSeg := segs[clearIdx]
		newName := clearSeg.path[:len(clearSeg.path)-len(".CLEAR")]
		if err = os.Rename(clearSeg.path, newName); err != nil {
			return nil, err
		}
		clearSeg.path = newName
		segs = append([]*Segment{}, clearSeg)
	}

	// create segment
	if len(segs) == 0 {
		seg, err := CreateSegment(1, path)
		if err != nil {
			return nil, err
		}
		segs = append(segs, seg)
	} else {
		err = segs[len(segs)-1].Load()
		if err != nil {
			return nil, err
		}
	}

	cache := NewCache(opts.SegmentNum, func(v interface{}) error {
		seg := v.(*Segment)
		seg.Close()
		return nil
	})
	return &Wal{
		opts:  opts,
		segs:  segs,
		path:  path,
		first: segs[0].First(),
		cache: cache,
	}, nil
}

func (w *Wal) curSeg() *Segment {
	return w.segs[len(w.segs)-1]
}

func (w *Wal) cycle() error {
	curSeg := w.curSeg()
	newSeg, err := CreateSegment(curSeg.Last()+1, w.path)
	if err != nil {
		return err
	}
	fmt.Printf("new segment: %#v\n", newSeg)
	// curSeg.Close()
	w.segs = append(w.segs, newSeg)
	w.cache.Push(curSeg)
	return nil
}

func (w *Wal) First() int64 {
	w.RLock()
	defer w.RUnlock()
	return w.first
}

func (w *Wal) Last() int64 {
	w.RLock()
	defer w.RUnlock()
	return w.curSeg().last
}

func (w *Wal) Append(entry Entry) error {
	w.Lock()
	defer w.Unlock()
	return w.append(Entries{entry})
}

func (w *Wal) AppendBatch(entries Entries) error {
	w.Lock()
	defer w.Unlock()
	return w.append(entries)
}

func (w *Wal) append(entries Entries) error {
	s := w.curSeg()
	var datas [][]byte
	for i, entry := range entries {
		if entry.Index != s.Last()+int64(i+1) {
			return ErrOutOfOrder
		}
		datas = append(datas, entry.Data)
	}

	err := s.Write(datas, w.opts.IsSync)
	if err != nil {
		return err
	}
	if s.Length() > w.opts.SegmentSize {
		if err = w.cycle(); err != nil {
			return err
		}
	}
	return nil
}

func (w *Wal) Get(idx int64) ([]byte, error) {
	w.RLock()
	defer w.RUnlock()

	if idx < w.first || idx > w.curSeg().Last() {
		return nil, ErrOutOfOrder
	}
	// current segment
	seg := w.curSeg()
	if idx < seg.first {
		seg = nil
		// cache segment
		rangeFunc := func(value interface{}) bool {
			vseg := value.(*Segment)
			if idx >= vseg.first &&
				idx < (vseg.first+int64(len(vseg.items))) {
				seg = vseg
				return false
			}
			return true
		}
		w.cache.Range(rangeFunc)

		// find all segment
		if seg == nil {
			w.cacheLock.Lock()
			defer w.cacheLock.Unlock()
			w.cache.Range(rangeFunc)
			if seg == nil {
				segIndex := w.selectSegment(idx)
				fmt.Printf("idx:%d select segindex: %d\n", idx, segIndex)
				seg = w.segs[segIndex]
				// defer seg.Close()
				if err := seg.Load(); err != nil {
					return nil, err
				}
				w.cache.Push(seg)
			}
		}
	}

	return seg.Read(idx)
}

func (w *Wal) selectSegment(idx int64) int {
	i, j := 0, len(w.segs)
	for i < j {
		h := i + (j-i)/2
		if idx >= w.segs[h].first {
			i = h + 1
		} else {
			j = h
		}
	}
	return i - 1
}

// TruncateFront remain include idx item
func (w *Wal) TruncateFront(idx int64) error {
	w.Lock()
	defer w.Unlock()

	if idx == w.curSeg().Last()+1 {
		return w.clearSegment(idx)
	}

	if idx <= w.first || idx > w.curSeg().Last()+1 {
		return ErrOutOfOrder
	}

	// select segment
	segIndex := w.selectSegment(idx)
	seg := w.segs[segIndex]
	if seg.data == nil {
		defer seg.Close()
		if err := seg.Load(); err != nil {
			return err
		}
	}
	item := seg.items[idx-seg.first]
	data := seg.data[item.pos:]

	// create temp file
	temp, err := w.createTempSegment(data)
	if err != nil {
		return err
	}
	// rename temp file to temp segment index file
	newName := path.Join(w.path, segmentName(idx)+".START")
	if err = os.Rename(temp, newName); err != nil {
		return err
	}
	// remove file
	for i := 0; i <= segIndex; i++ {
		err = os.Remove(w.segs[i].path)
		if err != nil {
			return err
		}
	}
	// rename temp segment index file to real segment file
	finalName := path.Join(w.path, segmentName(idx))
	if err = os.Rename(newName, finalName); err != nil {
		return err
	}
	// reset cache
	seg.data = data
	seg.items = seg.items[idx-seg.first:]
	seg.first = idx
	seg.path = finalName
	w.segs = append([]*Segment{}, w.segs[segIndex:]...)
	w.first = w.segs[0].first
	w.cache.Clear()
	return nil
}

// TruncateBack remain include idx item
func (w *Wal) TruncateBack(idx int64) error {
	w.Lock()
	defer w.Unlock()
	if idx == w.first-1 {
		return w.clearSegment(w.first)
	}

	if idx < w.first-1 || idx >= w.curSeg().Last() {
		return ErrOutOfOrder
	}

	// select segment
	segIndex := w.selectSegment(idx)
	seg := w.segs[segIndex]
	if seg.data == nil {
		defer seg.Close()
		if err := seg.Load(); err != nil {
			return err
		}
	}
	item := seg.items[idx-seg.first]
	data := seg.data[:item.end]

	// create temp file
	temp, err := w.createTempSegment(data)
	if err != nil {
		return err
	}
	// rename temp file to temp segment index file
	// here must be selected segment's first index
	newName := path.Join(w.path, segmentName(seg.first)+".END")
	if err = os.Rename(temp, newName); err != nil {
		return err
	}
	// remove file
	for i := segIndex; i < len(w.segs); i++ {
		// fmt.Printf("remove:%s\n", w.segs[i].path)
		err = os.Remove(w.segs[i].path)
		if err != nil {
			return err
		}
	}

	// rename temp segment index file to real segment file
	if err = os.Rename(
		newName, path.Join(w.path, segmentName(seg.first))); err != nil {
		return err
	}

	// reset cache
	seg.data = data
	seg.items = seg.items[:idx-seg.first+1]
	seg.last = idx
	w.segs = append([]*Segment{}, w.segs[:segIndex+1]...)
	w.cache.Clear()
	return nil
}

func (w *Wal) createTempSegment(data []byte) (string, error) {
	spath := filepath.Join(w.path, "TEMP")
	file, err := os.Create(spath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	n, err := file.Write([]byte(SegMeta))
	if err != nil || n != 4 {
		return "", fmt.Errorf("write segment meta error")
	}
	_, err = file.Write(data)
	if err != nil {
		return "", err
	}
	if err = file.Sync(); err != nil {
		return "", err
	}
	return spath, nil
}

func (w *Wal) clearSegment(idx int64) error {
	spath := path.Join(w.path, segmentName(idx)+".CLEAR")
	file, err := os.Create(spath)
	if err != nil {
		return err
	}
	defer file.Close()

	n, err := file.Write([]byte(SegMeta))
	if err != nil || n != 4 {
		return fmt.Errorf("write segment meta error")
	}
	if err = file.Sync(); err != nil {
		return err
	}

	for _, seg := range w.segs {
		err = os.Remove(seg.path)
		if err != nil {
			return err
		}
	}

	newName := spath[:len(spath)-len(".CLEAR")]
	if err = os.Rename(spath, newName); err != nil {
		return err
	}

	seg := &Segment{first: idx, path: newName}
	if err = seg.Load(); err != nil {
		return err
	}
	w.segs = append([]*Segment{}, seg)
	w.first = seg.first
	w.cache.Clear()
	return nil
}

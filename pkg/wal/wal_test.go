package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

var wal *Wal

func testCreateFile(path string, index int64, suffix string) error {
	spath := filepath.Join(path, segmentName(index)+suffix)
	file, err := os.Create(spath)
	if err != nil {
		return err
	}
	defer file.Close()

	n, err := file.Write([]byte(SegMeta))
	if err != nil || n != 4 {
		return fmt.Errorf("write segment meta error")
	}
	return nil
}

func TestWalNewWithStartTruncating(t *testing.T) {
	err := os.MkdirAll("./data", 0777)
	assert.Nil(t, err)
	err = testCreateFile("./data", 1, "")
	assert.Nil(t, err)
	err = testCreateFile("./data", 100, "")
	assert.Nil(t, err)
	err = testCreateFile("./data", 150, ".START")
	assert.Nil(t, err)
	err = testCreateFile("./data", 200, "")
	assert.Nil(t, err)

	wal, err = NewWal("./data", nil)
	assert.Nil(t, err)
	assert.EqualValues(t, wal.first, 150)
	assert.EqualValues(t, wal.curSeg().last, 199)
	assert.EqualValues(t, len(wal.segs), 2)
	os.RemoveAll("./data")
}

func TestWalNewWithEndTruncating(t *testing.T) {
	err := os.MkdirAll("./data", 0777)
	assert.Nil(t, err)
	err = testCreateFile("./data", 1, "")
	assert.Nil(t, err)
	err = testCreateFile("./data", 100, "")
	assert.Nil(t, err)
	err = testCreateFile("./data", 100, ".END")
	assert.Nil(t, err)
	err = testCreateFile("./data", 200, "")
	assert.Nil(t, err)

	wal, err = NewWal("./data", nil)
	assert.Nil(t, err)
	assert.EqualValues(t, wal.first, 1)
	assert.EqualValues(t, wal.curSeg().first, 100)
	assert.EqualValues(t, len(wal.segs), 2)
	os.RemoveAll("./data")
}

func TestWalAppendAndGet(t *testing.T) {
	var err error
	wal, err = NewWal("./data", nil)
	assert.Nil(t, err)
	assert.EqualValues(t, wal.first, 1)
	assert.EqualValues(t, len(wal.segs), 1)
	assert.NotNil(t, wal.segs[0].file)

	t.Run("test append", func(t *testing.T) {
		err = wal.Append(Entry{10, []byte("k1=v1")})
		assert.Equal(t, err, ErrOutOfOrder)
		err = wal.Append(Entry{1, []byte("k1=v1")})
		assert.Nil(t, err)
		err = wal.AppendBatch(Entries{{2, []byte("k2=v2")}, {3, []byte("k3=v3")}})
		assert.Nil(t, err)
		assert.EqualValues(t, wal.curSeg().Last(), 3)
		assert.EqualValues(t, len(wal.curSeg().items), 3)
	})

	t.Run("test get", func(t *testing.T) {
		_, err = wal.Get(0)
		assert.Equal(t, err, ErrOutOfOrder)
		for i := 1; i < 4; i++ {
			data, err := wal.Get(int64(i))
			assert.Nil(t, err)
			assert.Equal(t, string(data), fmt.Sprintf("k%d=v%d", i, i))
		}
	})
}

func TestWalCycle(t *testing.T) {
	wal.opts.SegmentSize = 20
	// cycle after this write
	wal.Append(Entry{4, []byte("k4=wwwwwwwwww")})
	assert.EqualValues(t, len(wal.segs), 2)
	assert.EqualValues(t, wal.segs[1].first, 5)
	assert.EqualValues(t, wal.segs[1].last, 4)

	err := wal.AppendBatch(Entries{{5, []byte("k5=xxxxxxxxxx")}, {6, []byte("k6=yyyyyyyyyy")}})
	assert.Nil(t, err)
	assert.EqualValues(t, len(wal.segs), 3)
	assert.EqualValues(t, wal.segs[2].first, 7)
	assert.EqualValues(t, wal.segs[2].last, 6)
}

func TestWalCache(t *testing.T) {
	//1234|56|.
	// wal.cache.size = 1
	// assert.EqualValues(t, wal.cache.l.Len(), 0)
	// data, err := wal.Get(2)
	// assert.Nil(t, err)
	// assert.Equal(t, string(data), "k2=v2")
	// assert.EqualValues(t, wal.cache.l.Len(), 2)
	// seg := wal.cache.l.Front().Value.(*Segment)
	// assert.EqualValues(t, seg.first, 1)

	// data, err = wal.Get(5)
	// assert.Nil(t, err)
	// assert.Equal(t, string(data), "k5=xxxxxxxxxx")
	// assert.EqualValues(t, wal.cache.l.Len(), 1)
	// seg = wal.cache.l.Front().Value.(*Segment)
	// assert.EqualValues(t, seg.first, 5)
}

func TestWalTruncateFront(t *testing.T) {
	//1234|56|789
	err := wal.AppendBatch(Entries{{7, []byte("k7=7")}, {8, []byte("k8=8")}, {9, []byte("k9=9")}})
	assert.Nil(t, err)
	assert.EqualValues(t, len(wal.segs), 3)
	assert.EqualValues(t, wal.segs[2].first, 7)
	assert.EqualValues(t, wal.segs[2].last, 9)

	err = wal.TruncateFront(3)
	assert.Nil(t, err)
	assert.EqualValues(t, wal.first, 3)
	assert.EqualValues(t, len(wal.segs), 3)

	for _, seg := range wal.segs {
		fmt.Printf("seg :%#v\n", seg)
	}
}

func TestWalTruncateBack(t *testing.T) {
	err := wal.TruncateBack(8)
	assert.Nil(t, err)
	assert.EqualValues(t, wal.first, 3)
	assert.EqualValues(t, wal.curSeg().last, 8)
	assert.EqualValues(t, len(wal.segs), 3)

	for _, seg := range wal.segs {
		fmt.Printf("seg :%#v\n", seg)
	}
}

func TestWalClearSegment(t *testing.T) {
	t.Run("clear segment back", func(t *testing.T) {
		err := wal.TruncateBack(2)
		assert.Nil(t, err)
		assert.EqualValues(t, wal.first, 3)
		assert.EqualValues(t, wal.curSeg().last, 2)
		assert.EqualValues(t, len(wal.segs), 1)
	})

	t.Run("clear segment front", func(t *testing.T) {
		err := wal.AppendBatch(Entries{{3, []byte("k3=3333333")}, {4, []byte("k4=4444444444444")}})
		assert.Nil(t, err)
		err = wal.AppendBatch(Entries{{5, []byte("k5=5")}, {6, []byte("k6=6")}})
		assert.Nil(t, err)
		assert.EqualValues(t, wal.curSeg().last, 6)
		assert.EqualValues(t, len(wal.segs), 2)

		err = wal.TruncateFront(7)
		assert.Nil(t, err)
		assert.EqualValues(t, wal.first, 7)
		assert.EqualValues(t, wal.curSeg().last, 6)
		assert.EqualValues(t, len(wal.segs), 1)
	})

}

func TestWalClear(t *testing.T) {
	wal, err := NewWal("./data", nil)
	assert.Nil(t, err)
	assert.EqualValues(t, wal.first, 7)
	assert.EqualValues(t, len(wal.segs), 1)
	os.RemoveAll("./data")
}

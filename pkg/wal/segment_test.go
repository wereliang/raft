package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateSegment(t *testing.T) {
	seg, err := CreateSegment(10, "./")
	assert.Nil(t, err)
	assert.EqualValues(t, seg.first, 10)
	assert.EqualValues(t, seg.last, 9)

	err = seg.Write([][]byte{[]byte("hello"), []byte("world")}, true)
	assert.Nil(t, err)
	assert.EqualValues(t, seg.last, 11)
	assert.EqualValues(t, len(seg.items), 2)

	data, err := seg.Read(10)
	assert.Nil(t, err)
	assert.EqualValues(t, string(data), "hello")

	data, err = seg.Read(11)
	assert.Nil(t, err)
	assert.EqualValues(t, string(data), "world")
}

func TestRecoverSegment(t *testing.T) {
	seg, err := RecoverSegment(segmentName(10), "./")
	assert.Nil(t, err)
	err = seg.Load()
	assert.Nil(t, err)

	assert.EqualValues(t, seg.last, 11)
	assert.EqualValues(t, len(seg.items), 2)

	data, err := seg.Read(10)
	assert.Nil(t, err)
	assert.EqualValues(t, string(data), "hello")

	data, err = seg.Read(11)
	assert.Nil(t, err)
	assert.EqualValues(t, string(data), "world")

	err = seg.Write([][]byte{[]byte("www")}, true)
	assert.Nil(t, err)
	assert.EqualValues(t, seg.last, 12)
	assert.EqualValues(t, len(seg.items), 3)
}

func TestSegmentClear(t *testing.T) {
	os.RemoveAll(filepath.Join("./", segmentName(10)))
}

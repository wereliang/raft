package log

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// var st *storage

func TestStorageAppend(t *testing.T) {
	log, err := NewStorage("./data")
	st := log.(*storage)
	assert.Nil(t, err)
	last, err := st.AppendLog([]*LogItem{
		{1, 1, []byte("111")},
		{1, 2, []byte("222")},
		{1, 3, []byte("333")},
		{1, 4, []byte("444")},
		{1, 5, []byte("555")},
	})
	assert.Nil(t, err)
	assert.EqualValues(t, last, 5)

	_, err = st.AppendLog([]*LogItem{{1, 7, []byte("777")}}, 7)
	assert.NotNil(t, err)
	assert.EqualValues(t, last, 5)

	last, err = st.AppendLog([]*LogItem{{1, 4, []byte("hello")}, {1, 5, []byte("world")}}, 4)
	assert.Nil(t, err)
	assert.EqualValues(t, last, 5)
	assert.EqualValues(t, st.wal.Last(), 5)

	for i := st.wal.First(); i <= st.wal.Last(); i++ {
		data, err := st.wal.Get(i)
		assert.Nil(t, err)
		var item LogItem
		// err = json.Unmarshal(data, &item)
		err = st.UnmarshalEntry(data, &item)
		assert.Nil(t, err)
		fmt.Printf("%d %#v\n", i, item)
		// assert.EqualValues(t, item.Index, i)

		if i == 4 {
			assert.Equal(t, string(item.Data), "hello")
		} else if i == 5 {
			assert.Equal(t, string(item.Data), "world")
		} else {
			assert.Equal(t, string(item.Data), fmt.Sprintf("%d%d%d", i, i, i))
		}
	}
}

func TestStorageDestory(t *testing.T) {
	os.RemoveAll("./data")
}

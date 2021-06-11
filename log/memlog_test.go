package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemLog(t *testing.T) {
	rlog := NewMemLog()
	items := []*LogItem{{1, 0, nil}, {1, 0, nil}, {1, 0, nil}}
	_, err := rlog.AppendLog(items)
	assert.Nil(t, err, "append log fail")
	assert.Equal(t, rlog.LastIndex(), int64(3))
	assert.Equal(t, items[0].Index, int64(1))
	assert.Equal(t, items[1].Index, int64(2))
	assert.Equal(t, items[2].Index, int64(3))

	_, err = rlog.AppendLog([]*LogItem{{1, 0, nil}}, 5)
	assert.NotNil(t, err, "append discontinuous item must be error")

	_, err = rlog.AppendLog([]*LogItem{{1, 0, nil}}, 4)
	assert.Nil(t, err, "append continuous item must be ok")
	rlog.Dump()

	_, err = rlog.AppendLog([]*LogItem{{1, 0, nil}})
	assert.Nil(t, err, "append continuous item must be ok")
	rlog.Dump()

	_, err = rlog.AppendLog([]*LogItem{{2, 0, nil}, {2, 0, nil}}, 2)
	assert.Nil(t, err, "append replace must be ok")

	last := rlog.LastIndex()
	assert.Equal(t, last, int64(3))
}

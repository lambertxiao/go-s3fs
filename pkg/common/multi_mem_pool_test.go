package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetData1(t *testing.T) {
	testSize := 15 * 1024 * 1024
	memPool := MMP.Get(int64(testSize))
	data, err := memPool.Get(int64(testSize))
	defer memPool.Put(data)

	assert.Nil(t, err)
	assert.Equal(t, 16*1024*1024, len(data))
}

func TestGetData2(t *testing.T) {
	testSize := 65 * 1024 * 1024
	data, err := MMP.GetData(int64(testSize))

	assert.Nil(t, err)
	assert.Equal(t, testSize, len(data))
}

func TestAddPool(t *testing.T) {
	mmp := NewMultiMemPool()
	mmp.Add(10 * 1024)
	mmp.Add(10 * 1024)
}

func BenchmarkXxx(b *testing.B) {
	testSize := 340 * 1024 * 1024
	memPool := MMP.Get(int64(testSize))
	for i := 0; i < b.N; i++ {
		data, _ := memPool.Get(int64(testSize))
		memPool.Put(data)
	}
}

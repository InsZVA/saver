package record

import (
	"math/rand"
	"os"
	"testing"

	"github.com/InsZVA/saver/util"
)

func newTestReader(t *testing.T, subfix string) *BaseReader {
	f, err := os.Open("/tmp/record_" + subfix)
	if err != nil {
		t.Error(err)
	}
	ret := &BaseReader{
		reader: f,
	}
	err = ret.nextBlock()
	if err != nil {
		t.Error(err)
	}
	return ret
}

func TestBaseReadRecord(t *testing.T) {
	// 正确的情况测试
	writer := newTestWriter(t, "read")
	rawData := [][]byte{
		[]byte("Hello, world"),
		util.RandomSlice(blockSize + 1),
		util.RandomSlice(blockSize - 1),
		util.RandomSlice(blockSize / 2),
		util.RandomSlice(75),
	}
	for _, raw := range rawData {
		writer.write(raw)
	}
	writer.flush()
	writer.curFile.Close()

	reader := newTestReader(t, "read")
	for _, raw := range rawData {
		data, err := reader.ReadRecord()
		if err != nil {
			t.Error(err)
		}
		expect(t, string(raw), string(data))
		t.Log(string(data))
	}

	// TODO: 测试错误的case

	// 测试大量数据
	writer = newTestWriter(t, "read_batch")
	// 插入大量随机数据测试
	datas := [][]byte{}
	for i := 0; i < 1000; i++ {
		datas = append(datas, util.RandomSlice(rand.Intn(int(blockSize+blockSize/2))))
		writer.write(datas[i])
	}
	writer.flush()
	reader = newTestReader(t, "read_batch")
	for i := 0; i < 1000; i++ {
		data, err := reader.ReadRecord()
		if err != nil {
			t.Error(err)
		}
		expect(t, datas[i], data)
	}
}

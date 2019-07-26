package record

import (
	"os"
	"testing"
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
		randomSlice(blockSize + 1),
		randomSlice(blockSize - 1),
		randomSlice(blockSize / 2),
		randomSlice(75),
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

	// 测试错误的case
}

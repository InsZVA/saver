package record

import (
	"os"
	"testing"
)

func newTestReader(t *testing.T) *BaseReader {
	f, err := os.Open("/tmp/record")
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

func TestBaseReaderNext(t *testing.T) {
	writer := newTestWriter(t)
	writer.write([]byte("Hello, world"))
	writer.write(randomSlice(10882))
	writer.write(randomSlice(76542))
	writer.write(randomSlice(87655))
	writer.write(randomSlice(75))
	writer.flush()
	writer.curFile.Close()

	reader := newTestReader(t)
	data, err := reader.ReadRecord()
	if err != nil {
		t.Error(err)
	}
	t.Log(string(data))
	data, err = reader.ReadRecord()
	if err != nil {
		t.Error(err)
	}
	t.Log(string(data))
	data, err = reader.ReadRecord()
	if err != nil {
		t.Error(err)
	}
	t.Log(string(data))
	data, err = reader.ReadRecord()
	if err != nil {
		t.Error(err)
	}
	t.Log(string(data))
	data, err = reader.ReadRecord()
	if err != nil {
		t.Error(err)
	}
	t.Log(string(data))
}

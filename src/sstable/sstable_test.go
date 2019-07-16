package sstable

import (
	"table"
	"testing"
)

func NewTestSkipList() *table.SkipList {
	list := table.NewSkipList()
	list.Set(table.NewKey([]byte("a")), []byte{1})
	list.Set(table.NewKey([]byte("b")), []byte{2})
	list.Set(table.NewKey([]byte("c")), []byte{3})
	return list
}

func TestSSTable(t *testing.T) {
	list := NewTestSkipList()
	sst, err := CreateSSTable("/tmp/sst0")
	if err != nil {
		t.Error(err)
	}
	err = sst.FromMemTable(list)
	if err != nil {
		t.Error(err)
	}
	sst.Close()
}

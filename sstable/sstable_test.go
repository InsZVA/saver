package sstable

import (
	"testing"

	"github.com/InsZVA/saver/table"
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

	sst, err = OpenSSTable("/tmp/sst0")
	if err != nil {
		t.Error(err)
	}
	reader := sst.NewReader()
	i, err := reader.Find(table.NewKey([]byte("c")))
	if err != nil {
		t.Error(err)
	}
	t.Log(i.Next())
	t.Log(i.key.Key(), i.val)

}

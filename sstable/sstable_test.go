package sstable

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/InsZVA/saver/table"
	"github.com/InsZVA/saver/util"
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
	reader, err := sst.NewReader()
	if err != nil {
		t.Error(err)
	}
	i, err := reader.Find(table.NewKey([]byte("b")))
	if err != nil {
		t.Error(err)
	}
	if !i.Next() {
		t.Error("没有找到c")
	}
	if !bytes.Equal(i.key.Key(), []byte("b")) {
		t.Error("找到的key错误")
	}
	if !bytes.Equal(i.val, []byte{2}) {
		t.Error("找到的val错误")
	}
	if !i.Next() {
		t.Error("b之后的c没有next读到")
	}
	if !bytes.Equal(i.key.Key(), []byte("c")) {
		t.Error("next找到的key错误")
	}
	if !bytes.Equal(i.val, []byte{3}) {
		t.Error("next找到的val错误")
	}
	if i.Next() {
		t.Error("c后还有next")
	}

	// 大量插入和查询测试
	keys := []table.Key{}
	vals := [][]byte{}
	for i := 0; i < 1000; i++ {
		keys = append(keys, table.NewKey(util.RandomSlice(64)))
		vals = append(vals, util.RandomSlice(128))
		list.Set(keys[i], vals[i])
	}
	sst, err = CreateSSTable("/tmp/sst1")
	if err != nil {
		t.Error(err)
	}
	err = sst.FromMemTable(list)
	if err != nil {
		t.Error(err)
	}
	sst.Close()

	sst, err = OpenSSTable("/tmp/sst1")
	if err != nil {
		t.Error(err)
	}
	reader, err = sst.NewReader()
	if err != nil {
		t.Error(err)
	}
	// 随机查询
	for j := 0; j < 1000; j++ {
		idx := rand.Intn(1000)
		i, err = reader.Find(keys[idx])
		if err != nil {
			t.Error(err)
		}
		if !i.Next() {
			t.Error(keys[idx].Key(), "没有发现")
		}
		if !bytes.Equal(i.val, vals[idx]) {
			t.Error(keys[idx].Key(), "值错误")
		}
	}
}

package table

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/InsZVA/saver/util"
)

func TestKeyCmp(t *testing.T) {
	if NewKey([]byte{1, 2, 3}).Cmp(NewKey([]byte{1, 2, 4})) >= 0 ||
		NewKey([]byte{1, 2, 3}).Cmp(NewKey([]byte{1, 2, 3, 4})) >= 0 ||
		NewKey([]byte{}).Cmp(NewKey([]byte{1})) >= 0 {
		t.Error("键小于判断错误")
	}
	if NewKey([]byte{2, 2, 2}).Cmp(NewKey([]byte{2, 2, 2})) != 0 ||
		NewKey([]byte{}).Cmp(NewKey([]byte{})) != 0 {
		t.Error("键等于判断错误")
	}
	if NewKey([]byte{3, 2, 1}).Cmp(NewKey([]byte{3, 2, 0})) <= 0 ||
		NewKey([]byte{3, 2, 1}).Cmp(NewKey([]byte{3, 2})) <= 0 ||
		NewKey([]byte{1}).Cmp(NewKey([]byte{})) <= 0 {
		t.Error("键大于判断错误")
	}
}

func TestKeyKey(t *testing.T) {
	k := NewKey([]byte{1, 2, 3})
	if !bytes.Equal(k.key, k.Key()) {
		t.Error("Key.Key()方法返回值错误")
	}
}

func TestSkipListFirst(t *testing.T) {
	l := NewSkipList()
	if l.First() != l.start[0] {
		t.Error("list.first错误")
	}
}

func TestSkipListEnd(t *testing.T) {
	l := NewSkipList()
	if l.End() != l.end[0] {
		t.Error("list.end错误")
	}
}

func TestSkipList(t *testing.T) {
	l := NewSkipList()
	l.insert(NewKey([]byte("a")), []byte{1})
	t.Log(l.String())
	l.insert(NewKey([]byte("b")), []byte{1, 2, 3})
	l.insert(NewKey([]byte("c")), []byte{1, 2})
	nodeA, found := l.Find(NewKey([]byte("a")))
	if !found {
		t.Error("没有发现a")
	}
	if !bytes.Equal(nodeA[0].Key().key, []byte("a")) {
		t.Error("ListNode的Key错误")
	}
	if !bytes.Equal(nodeA[0].Val(), []byte{1}) {
		t.Error("ListNode的Val错误")
	}
	l.insert(NewKey([]byte("a")), []byte{88})
	if !bytes.Equal(nodeA[0].Val(), []byte{88}) {
		t.Error("ListNode的Val没有成功更新")
	}
	nodeB, found := l.Find(NewKey([]byte("b")))
	if !found {
		t.Error("没有发现b")
	}
	if nodeB[0].Prev() != nodeA[0] {
		t.Error("ListNode的顺序错误")
	}
	nodeC, found := l.Find(NewKey([]byte("c")))
	if !found {
		t.Error("没有发现c")
	}
	if nodeB[0].Next() != nodeC[0] {
		t.Error("ListNode的顺序错误")
	}
	nodeD, found := l.Find(NewKey([]byte("d")))
	if found {
		t.Error("发现了d")
	}
	if nodeD[0] != nodeC[0] {
		t.Error("寻找d应该发现c")
	}
	// 大量插入和查询测试
	keys := []Key{}
	vals := [][]byte{}
	for i := 0; i < 1000; i++ {
		keys = append(keys, NewKey(util.RandomSlice(64)))
		vals = append(vals, util.RandomSlice(128))
		l.Set(keys[i], vals[i])
	}
	// 随机查询
	for i := 0; i < 1000; i++ {
		idx := rand.Intn(1000)
		node, found := l.Find(keys[idx])
		if !found {
			t.Error(keys[idx].key, "没有发现")
		}
		if !bytes.Equal(node[0].Val(), vals[idx]) {
			t.Error(keys[idx].key, "值错误")
		}
	}
}

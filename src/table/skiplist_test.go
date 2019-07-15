package table

import (
	"testing"
)

func TestSkipListInsert(t *testing.T) {
	l := NewSkipList()
	l.insert(NewKey([]byte("a")), []byte{1})
	t.Log(l)
	l.insert(NewKey([]byte("b")), []byte{1, 2, 3})
	t.Log(l)
	l.insert(NewKey([]byte("c")), []byte{1, 2})
	t.Log(l)
	l.insert(NewKey([]byte("a")), []byte{88})
	t.Log(l)
	t.Log(l.Find(NewKey([]byte("a"))))
	t.Log(l.Find(NewKey([]byte("b"))))
	t.Log(l.Find(NewKey([]byte("c"))))
}

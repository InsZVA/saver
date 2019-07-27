package table

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"unsafe"
)

const (
	maxLevel = 8
)

type Key struct {
	key []byte
	// TODO: MVCC支持
}

func (key Key) Key() []byte {
	return key.key
}

func NewKey(k []byte) Key {
	return Key{k}
}

func (key Key) Cmp(key2 Key) int {
	return bytes.Compare(key.key, key2.key)
}

type SkipListNode struct {
	key  Key
	val  []byte
	next *SkipListNode
	prev *SkipListNode
	down *SkipListNode
}

func (node *SkipListNode) String() string {
	if node.next != nil {
		return fmt.Sprintf("Node%x{key:%v,val:%v,next:%v}", uintptr(unsafe.Pointer(node)), node.key.key, node.val, node.next.key)
	}
	return fmt.Sprintf("Node{key:%v,val:%v}", node.key.key, node.val)
}

func (node *SkipListNode) Key() Key {
	return node.key
}

func (node *SkipListNode) Val() []byte {
	return node.val
}

func (node *SkipListNode) Prev() *SkipListNode {
	return node.prev
}

func (node *SkipListNode) Next() *SkipListNode {
	return node.next
}

type SkipList struct {
	start [maxLevel]*SkipListNode
	end   [maxLevel]*SkipListNode
}

func NewSkipList() *SkipList {
	ret := &SkipList{}
	for i := 0; i < maxLevel; i++ {
		// 虚头结点
		ret.start[i] = new(SkipListNode)
		ret.end[i] = new(SkipListNode)
		ret.start[i].next = ret.end[i]
		ret.end[i].prev = ret.start[i]
		if i > 0 {
			ret.start[i].down = ret.start[i-1]
			ret.end[i].down = ret.end[i-1]
		}
	}
	return ret
}

func (list *SkipList) First() *SkipListNode {
	return list.start[0]
}

func (list *SkipList) End() *SkipListNode {
	return list.end[0]
}

func (list *SkipList) String() string {
	ret := []string{}
	for i := maxLevel - 1; i >= 0; i-- {
		cur := []string{}
		p := list.start[i]
		for p != nil {
			cur = append(cur, p.String())
			p = p.next
		}
		ret = append(ret, strings.Join(cur, " -> "))
	}
	return strings.Join(ret, "\n")
}

// 返回每一层小于等于该Key的元素中最大的
func (list *SkipList) Find(key Key) ([maxLevel]*SkipListNode, bool) {
	level := maxLevel - 1
	p := list.start[level]
	var ret [maxLevel]*SkipListNode
	for p.next != nil {
		cmp := p.next.key.Cmp(key)
		// 虚拟结束结点大于一切结点
		if p.next == list.end[level] {
			cmp = 1
		}
		if cmp == 0 {
			ret[level] = p.next
			p = p.next
			for level > 0 {
				ret[level-1] = p.down
				p = p.down
				level--
			}
			return ret, true
		} else if cmp < 0 {
			p = p.next
		} else {
			ret[level] = p
			if p.down != nil {
				level -= 1
				p = p.down
			} else {
				return ret, false
			}
		}
	}
	return ret, false
}

func (list *SkipList) randomLevel() int {
	l := 1
	for l < maxLevel {
		if rand.Float32() < 0.5 {
			l += 1
		} else {
			break
		}
	}
	return l
}

func (list *SkipList) insert(key Key, val []byte) {

	nodes, found := list.Find(key)
	if found {
		for _, node := range nodes {
			node.val = val
		}
		return
	}

	l := list.randomLevel()
	for i := 0; i < l; i++ {
		new_node := new(SkipListNode)
		new_node.key = key
		new_node.val = val
		new_node.prev = nodes[i]
		new_node.next = nodes[i].next
		nodes[i].next.prev = new_node
		nodes[i].next = new_node
		if i > 0 {
			new_node.down = nodes[i-1].next
		}
	}

	return
}

func (list *SkipList) Set(key Key, val []byte) {
	list.insert(key, val)
}

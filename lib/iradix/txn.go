package iradix

import (
	"bytes"
	"fastdb/utils"
	"github.com/hashicorp/golang-lru/v2/simplelru"
)

type Txn[T any] struct {
	// 根节点
	root *Node[T]

	size int

	// writable 在一个事务中，不能直接对节点修改，否则会导致脏读破坏事务的一致性
	writable *simplelru.LRU[*Node[T], any]
}

func (t *Tree[T]) Txn() *Txn[T] {
	txn := &Txn[T]{
		root: t.root,
		size: t.size,
	}
	return txn
}

func (t *Txn[T]) Insert(k []byte, v T) (T, bool) {
	newRoot, oldVal, didUpdate := t.insert(t.root, k, k, v)
	if newRoot != nil {
		t.root = newRoot
	}
	if !didUpdate {
		t.size++
	}

	return oldVal, didUpdate
}

func (t *Txn[T]) insert(n *Node[T], k, search []byte, v T) (*Node[T], T, bool) {
	var zero T

	// 说明搜索已经结束了，开始新增节点
	if len(search) == 0 {
		var oldVal T
		didUpdate := false
		if n.isLeaf() {
			oldVal = n.leaf.val
			didUpdate = true
		}
		nc := t.writeNode(n, true)
		nc.leaf = &leafNode[T]{
			key: k,
			val: v,
		}
		return nc, oldVal, didUpdate
	}

	idx, child := n.getEdge(search[0])

	if child == nil {
		e := edge[T]{
			label: search[0],
			node: &Node[T]{
				leaf: &leafNode[T]{
					key: k,
					val: v,
				},
				prefix: search,
			},
		}
		nc := t.writeNode(n, false)
		nc.addEdge(e)
		return nc, zero, false
	}

	commonPrefix := utils.LongestPrefix(search, child.prefix)
	if commonPrefix == len(child.prefix) {
		search = search[commonPrefix:]
		newChild, oldVal, didUpdate := t.insert(child, k, search, v)
		if newChild != nil {
			nc := t.writeNode(n, false)
			nc.edges[idx].node = newChild
			return nc, oldVal, didUpdate
		}
		return nil, oldVal, didUpdate
	}

	// 需要分裂节点
	nc := t.writeNode(n, false)
	splitNode := &Node[T]{
		prefix: search[:commonPrefix],
	}
	nc.replaceEdge(edge[T]{
		label: search[0],
		node:  splitNode,
	})

	modChild := t.writeNode(child, false)
	splitNode.addEdge(edge[T]{
		label: modChild.prefix[commonPrefix],
		node:  modChild,
	})
	modChild.prefix = modChild.prefix[commonPrefix:]

	leaf := &leafNode[T]{
		key: k,
		val: v,
	}

	search = search[commonPrefix:]
	if len(search) == 0 {
		splitNode.leaf = leaf
		return nc, zero, false
	}

	splitNode.addEdge(edge[T]{
		label: search[0],
		node: &Node[T]{
			leaf:   leaf,
			prefix: search,
		},
	})
	return nc, zero, false
}

func (t *Txn[T]) writeNode(n *Node[T], forLeafUpdate bool) *Node[T] {
	if t.writable == nil {
		lru, err := simplelru.NewLRU[*Node[T], any](defaultModifiedCache, nil)
		if err != nil {
			panic(err)
		}
		t.writable = lru
	}

	// 如果该node是在本次事务中生成的，直接使用缓存即可，无需重新生成
	if _, ok := t.writable.Get(n); ok {
		return n
	}

	nc := &Node[T]{
		leaf: n.leaf,
	}
	if n.prefix != nil {
		nc.prefix = make([]byte, len(n.prefix))
		copy(nc.prefix, n.prefix)
	}
	if len(n.edges) != 0 {
		nc.edges = make(edges[T], len(n.edges))
		copy(nc.edges, n.edges)
	}

	t.writable.Add(nc, nil)
	return nc
}

func (t *Txn[T]) Delete(k []byte) (T, bool) {
	var zero T
	newRoot, leaf := t.delete(t.root, k)
	if newRoot != nil {
		t.root = newRoot
	}
	if leaf != nil {
		t.size--
		return leaf.val, true
	}
	return zero, false
}

func (t *Txn[T]) delete(n *Node[T], search []byte) (*Node[T], *leafNode[T]) {
	if len(search) == 0 {
		if !n.isLeaf() {
			return nil, nil
		}

		oldLeaf := n.leaf
		nc := t.writeNode(n, true)
		nc.leaf = nil

		if n != t.root && len(nc.edges) == 1 {
			t.mergeChild(nc)
		}
		return nc, oldLeaf
	}

	label := search[0]
	idx, child := n.getEdge(label)
	if child == nil || !bytes.HasPrefix(search, child.prefix) {
		return nil, nil
	}

	search = search[len(child.prefix):]
	newChild, leaf := t.delete(child, search)
	if newChild == nil {
		return nil, nil
	}

	nc := t.writeNode(n, false)
	if newChild.leaf == nil && len(newChild.edges) == 0 {
		nc.delEdge(label)
		if n != t.root && len(nc.edges) == 1 && !nc.isLeaf() {
			t.mergeChild(nc)
		}
	} else {
		nc.edges[idx].node = newChild
	}
	return nc, leaf
}

func (t *Txn[T]) mergeChild(n *Node[T]) {
	e := n.edges[0]
	child := e.node

	n.prefix = utils.Concat(n.prefix, child.prefix)
	n.leaf = child.leaf
	if len(child.edges) != 0 {
		n.edges = make(edges[T], len(child.edges))
		copy(n.edges, child.edges)
	} else {
		n.edges = nil
	}
}

func (t *Txn[T]) Commit() *Tree[T] {
	return t.CommitOnly()
}

func (t *Txn[T]) CommitOnly() *Tree[T] {
	nt := &Tree[T]{t.root, t.size}
	t.writable = nil
	return nt
}

func (t *Txn[T]) Get(k []byte) (T, bool) {
	val, ok := t.root.get(k)
	return val, ok
}

package iradix

import (
	"bytes"
	"sort"
)

type leafNode[T any] struct {
	key []byte
	val T
}

type Node[T any] struct {

	// 如果该节点是叶子节点，该节点会有值
	leaf *leafNode[T]

	// prefix 这个节点的公共前缀
	prefix []byte

	// 父节点到子节点的边
	edges edges[T]
}

func (n *Node[T]) isLeaf() bool {
	return n.leaf != nil
}

func (n *Node[T]) getEdge(label byte) (int, *Node[T]) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= label
	})
	if idx < num && n.edges[idx].label == label {
		return idx, n.edges[idx].node
	}
	return -1, nil
}

func (n *Node[T]) addEdge(e edge[T]) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= e.label
	})
	n.edges = append(n.edges, e)
	if idx != num {
		copy(n.edges[idx+1:], n.edges[idx:num])
		n.edges[idx] = e
	}
}

func (n *Node[T]) replaceEdge(e edge[T]) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= e.label
	})
	if idx < num && n.edges[idx].label == e.label {
		n.edges[idx].node = e.node
		return
	}
	panic("replacing missing edge")

}

func (n *Node[T]) delEdge(label byte) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= label
	})
	if idx < num && n.edges[idx].label == label {
		copy(n.edges[idx:], n.edges[idx+1:])
		n.edges[len(n.edges)-1] = edge[T]{}
		n.edges = n.edges[:len(n.edges)-1]
	}
}

func (n *Node[T]) get(k []byte) (T, bool) {
	now := n
	search := k
	for {
		if len(search) == 0 {
			if now.isLeaf() {
				return now.leaf.val, true
			}
			break
		}

		_, now = now.getEdge(search[0])
		if now == nil {
			break
		}

		if bytes.HasPrefix(search, now.prefix) {
			search = search[len(now.prefix):]
		} else {
			break
		}
	}
	var zero T
	return zero, false
}

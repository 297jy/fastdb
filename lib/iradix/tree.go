package iradix

const (
	defaultModifiedCache = 8192
)

type Tree[T any] struct {
	root *Node[T]
	size int
}

func NewTree[T any]() *Tree[T] {
	t := &Tree[T]{
		root: &Node[T]{},
	}
	return t
}

func (t *Tree[T]) Insert(k []byte, v T) (*Tree[T], T, bool) {
	txn := t.Txn()
	old, ok := txn.Insert(k, v)
	return txn.Commit(), old, ok
}

func (t *Tree[T]) Delete(k []byte) (*Tree[T], T, bool) {
	txn := t.Txn()
	old, ok := txn.Delete(k)
	return txn.Commit(), old, ok
}

func (t *Tree[T]) Get(k []byte) (T, bool) {
	return t.root.get(k)
}

func (t *Tree[T]) Len() int {
	return t.size
}

func (t *Tree[T]) Minimum() ([]byte, T, bool) {
	now := t.root
	for {
		if now.isLeaf() {
			return now.leaf.key, now.leaf.val, true
		}
		if len(now.edges) > 0 {
			now = now.edges[0].node
		} else {
			break
		}
	}
	var zero T
	return nil, zero, false
}

func (t *Tree[T]) Maximum() ([]byte, T, bool) {
	now := t.root
	for {
		if num := len(now.edges); num > 0 {
			now = now.edges[num-1].node
			continue
		}
		if now.isLeaf() {
			return now.leaf.key, now.leaf.val, true
		} else {
			break
		}
	}
	var zero T
	return nil, zero, false
}

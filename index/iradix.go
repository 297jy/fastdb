package index

import (
	"fastdb/lib/iradix"
	"fastdb/wal"
	"sync"
)

type IRadixTree struct {
	tree *iradix.Tree[*wal.ChunkPosition]
	lock sync.Mutex
}

func newRadixTree() *IRadixTree {
	return &IRadixTree{
		tree: iradix.NewTree[*wal.ChunkPosition](),
	}
}

func (irx *IRadixTree) Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition {
	irx.lock.Lock()
	defer irx.lock.Unlock()
	var oldPos *wal.ChunkPosition
	irx.tree, oldPos, _ = irx.tree.Insert(key, position)
	return oldPos
}

func (irx *IRadixTree) Get(key []byte) *wal.ChunkPosition {
	pos, _ := irx.tree.Get(key)
	return pos
}

func (irx *IRadixTree) Delete(key []byte) (*wal.ChunkPosition, bool) {
	irx.lock.Lock()
	defer irx.lock.Unlock()
	var oldPos *wal.ChunkPosition
	var ok bool
	irx.tree, oldPos, ok = irx.tree.Delete(key)
	return oldPos, ok
}

func (irx *IRadixTree) Size() int {
	return irx.tree.Len()
}

type IRadixTreeIterator struct {
	options      IteratorOptions
	min          []byte
	max          []byte
	currentKey   []byte
	currentValue *wal.ChunkPosition
	tree         *iradix.Tree[*wal.ChunkPosition]
	iter         *iradix.Iterator[*wal.ChunkPosition]
}

/**
func (irx *IRadixTree) Iterator(options IteratorOptions) Iterator {
	minKey,_,_ := irx.tree.Minimum()
	maxKey,_,_ := irx.tree.Maximum()

} **/

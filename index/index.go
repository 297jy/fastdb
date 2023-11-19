package index

import "fastdb/wal"

type IteratorOptions struct {
	// 根据前缀来过滤key
	Prefix []byte
	// 代表迭代器的方向是否反转
	Reverse bool
}

type Iterator interface {
	// Rewind 设置迭代器指向第一个key
	Rewind()
	// Seek 将迭代器指向 传入的 key
	Seek(key []byte)

	Next(key []byte)

	Key() []byte

	Value() *wal.ChunkPosition

	Valid() bool

	Close()
}

type Indexer interface {
	Put(k []byte, position *wal.ChunkPosition) *wal.ChunkPosition

	Delete(k []byte) (*wal.ChunkPosition, bool)

	Get(k []byte) *wal.ChunkPosition

	Size() int
}

type IndexerType = byte

const (
	RadixTree IndexerType = iota
)

var indexType = RadixTree

func NewIndexer() Indexer {
	switch indexType {
	case RadixTree:
		return newRadixTree()
	default:
		panic("unexpected index type")
	}
}

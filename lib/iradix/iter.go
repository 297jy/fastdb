package iradix

type Iterator[T any] struct {
	node  *Node[T]
	stack []edges[T]
}

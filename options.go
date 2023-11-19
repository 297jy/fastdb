package fastdb

type BatchOptions struct {
	Sync     bool
	ReadOnly bool
}

type DbOptions struct {
	DirPath string
	// SegmentSize specifies the maximum size of each segment file in bytes.
	SegmentSize int64
	// BlockCache specifies the size of the block cache in number of bytes.
	BlockCache uint32

	Sync bool

	BytesPerSync uint32
}

package fastdb

import "os"

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

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = DbOptions{
	DirPath:      tempDBDir(),
	SegmentSize:  1 * GB,
	BlockCache:   64 * MB,
	Sync:         false,
	BytesPerSync: 0,
}

var DefaultBatchOptions = BatchOptions{
	Sync:     true,
	ReadOnly: false,
}

func tempDBDir() string {
	dir, _ := os.MkdirTemp("", "rosedb-temp")
	return dir
}

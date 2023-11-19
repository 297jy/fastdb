package wal

import "os"

type Options struct {
	// WAL segment 文件存放的目录
	DirPath string

	// SegmentSize WAL segment 文件的最大字节数
	SegmentSize int64

	// 指定 WAL segment的 扩展名称
	SegmentFileExt string

	// BlockCache 指定 一个 block cache 的字节数
	BlockCache uint32

	Sync bool

	// BytesPerSync 指定 在调用 fsync函数之前，应写入的 字节数
	BytesPerSync uint32
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = Options{
	DirPath:        os.TempDir(),
	SegmentSize:    GB,
	SegmentFileExt: ".SEG",
	BlockCache:     32 * KB * 10,
	Sync:           false,
	BytesPerSync:   0,
}

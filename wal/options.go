package wal

import (
	"fastdb/config"
	"os"
)

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

var DefaultOptions = Options{
	DirPath:        os.TempDir(),
	SegmentSize:    config.GB,
	SegmentFileExt: ".SEG",
	BlockCache:     32 * config.KB * 10,
	Sync:           false,
	BytesPerSync:   0,
}

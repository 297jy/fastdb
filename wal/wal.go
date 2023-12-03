package wal

import (
	"errors"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	initialSegmentFileID = 1
)

var (
	ErrValueTooLarge = errors.New("the data size can't larger than segment size")
)

type WAL struct {
	// activeSegment 代表目前正在写入 WAL segment文件
	activeSegment *segment
	// olderSegments 存放已经停止写入的 WAL segment 文件
	olderSegments map[SegmentID]*segment
	options       Options
	mu            sync.RWMutex
	blockCache    *lru.Cache[uint64, []byte]
	bytesWrite    uint32
}

type Reader struct {
	segmentReaders []*segmentReader
	currentReader  int
}

func (wal *WAL) Read(pos *ChunkPosition) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	var segment *segment
	if pos.SegmentId == wal.activeSegment.id {
		segment = wal.activeSegment
	} else {
		segment = wal.olderSegments[pos.SegmentId]
	}

	if segment == nil {
		return nil, fmt.Errorf("segment file %d%s not found", pos.SegmentId, wal.options.SegmentFileExt)
	}

	return segment.Read(pos.BlockNumber, pos.ChunkOffset)
}

func (wal *WAL) Write(data []byte) (*ChunkPosition, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	if int64(len(data))+chunkHeaderSize > wal.options.SegmentSize {
		return nil, ErrValueTooLarge
	}

	// if the active segment file is full, sync it and create a new one.
	if wal.isFull(int64(len(data))) {
		if err := wal.activeSegment.Sync(); err != nil {
			return nil, err
		}
		wal.bytesWrite = 0
		segment, err := openSegmentFile(wal.options.DirPath, wal.options.SegmentFileExt, wal.activeSegment.id+1, wal.blockCache)
		if err != nil {
			return nil, err
		}
		wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
		wal.activeSegment = segment
	}

	position, err := wal.activeSegment.Write(data)
	if err != nil {
		return nil, err
	}

	// 更新一下wal 还未刷新到硬盘中的 总字节数
	wal.bytesWrite += position.ChunkSize

	// sync the active segment file if needed.
	var needSync = wal.options.Sync
	if !needSync && wal.options.BytesPerSync > 0 {
		needSync = wal.bytesWrite >= wal.options.BytesPerSync
	}
	if needSync {
		if err := wal.activeSegment.Sync(); err != nil {
			return nil, err
		}
		wal.bytesWrite = 0
	}
	return position, nil
}

func (wal *WAL) isFull(delta int64) bool {
	return wal.activeSegment.Size()+delta+chunkHeaderSize > wal.options.SegmentSize
}

func openSegmentFile(dirPath, extName string, id uint32, cache *lru.Cache[uint64, []byte]) (*segment, error) {
	fd, err := os.OpenFile(
		SegmentFileName(dirPath, extName, id),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		fileModePerm,
	)
	if err != nil {
		return nil, err
	}

	offset, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		panic(fmt.Errorf("seek to the end of segment file %d%s failed: %v", id, extName, err))
	}

	return &segment{
		id:                 id,
		fd:                 fd,
		cache:              cache,
		currentBlockNumber: uint32(offset / blockSize),
		currentBlockSize:   uint32(offset % blockSize),
	}, nil
}

// SegmentFileName returns the file name of a segment file.
func SegmentFileName(dirPath string, extName string, id SegmentID) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+extName, id))
}

func (wal *WAL) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.activeSegment.Sync()
}

func Open(options Options) (*WAL, error) {
	if !strings.HasPrefix(options.SegmentFileExt, ".") {
		return nil, fmt.Errorf("segment file extension must start with '.'")
	}
	wal := &WAL{
		options:       options,
		olderSegments: make(map[SegmentID]*segment),
	}
	// create the directory if not exists.
	if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
		return nil, err
	}

	if options.BlockCache > 0 {
		var lruSize = options.BlockCache / blockSize
		if options.BlockCache%blockSize != 0 {
			lruSize += 1
		}
		cache, err := lru.New[uint64, []byte](int(lruSize))
		if err != nil {
			return nil, err
		}
		wal.blockCache = cache
	}

	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}

	var segmentIDs []int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id int
		_, err := fmt.Sscanf(entry.Name(), "%d"+options.SegmentFileExt, &id)
		if err != nil {
			continue
		}
		segmentIDs = append(segmentIDs, id)
	}

	if len(segmentIDs) == 0 {
		segment, err := openSegmentFile(options.DirPath, options.SegmentFileExt,
			initialSegmentFileID, wal.blockCache)
		if err != nil {
			return nil, err
		}
		wal.activeSegment = segment
	} else {
		sort.Ints(segmentIDs)
		for i, segId := range segmentIDs {
			segment, err := openSegmentFile(options.DirPath, options.SegmentFileExt,
				uint32(segId), wal.blockCache)
			if err != nil {
				return nil, err
			}
			if i == len(segmentIDs)-1 {
				wal.activeSegment = segment
			} else {
				wal.olderSegments[segment.id] = segment
			}
		}
	}
	return wal, nil

}

func (wal *WAL) NewReader() *Reader {
	return wal.NewReaderWithMax(0)
}

func (wal *WAL) NewReaderWithMax(segId SegmentID) *Reader {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	var segmentReaders []*segmentReader
	for _, segment := range wal.olderSegments {
		if segId == 0 || segment.id <= segId {
			reader := segment.NewReader()
			segmentReaders = append(segmentReaders, reader)
		}
	}
	if segId == 0 || wal.activeSegment.id <= segId {
		reader := wal.activeSegment.NewReader()
		segmentReaders = append(segmentReaders, reader)
	}

	sort.Slice(segmentReaders, func(i, j int) bool {
		return segmentReaders[i].segment.id < segmentReaders[j].segment.id
	})

	return &Reader{
		segmentReaders: segmentReaders,
		currentReader:  0,
	}
}

func (r *Reader) CurrentSegmentId() SegmentID {
	return r.segmentReaders[r.currentReader].segment.id
}

func (r *Reader) Next() ([]byte, *ChunkPosition, error) {
	if r.currentReader >= len(r.segmentReaders) {
		return nil, nil, io.EOF
	}
	data, position, err := r.segmentReaders[r.currentReader].Next()
	if err == io.EOF {
		r.currentReader++
		return r.Next()
	}
	return data, position, err
}

func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.blockCache != nil {
		wal.blockCache.Purge()
	}

	// close all segment files.
	for _, segment := range wal.olderSegments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	wal.olderSegments = nil

	// close the active segment file.
	return wal.activeSegment.Close()
}

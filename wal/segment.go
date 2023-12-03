package wal

import (
	"encoding/binary"
	"errors"
	"fastdb"
	lru "github.com/hashicorp/golang-lru/v2"
	"hash/crc32"
	"io"
	"os"
)

var (
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
)

type ChunkType = byte
type SegmentID = uint32

const (
	ChunkTypeFull ChunkType = iota
	ChunkTypeFirst
	ChunkTypeMiddle
	ChunkTypeLast
)

const (
	// 7 Bytes
	// Checksum Length Type
	//    4      2     1
	chunkHeaderSize = 7

	// 32 KB
	blockSize = 32 * fastdb.KB

	fileModePerm = 0644
)

type ChunkPosition struct {
	SegmentId SegmentID

	// 一个chunk中块的数量
	BlockNumber uint32

	// Chunk 在一个segment 文件中 的偏移量
	ChunkOffset int64

	// ChunkSize 一个 LogRecord 所占的字节数
	ChunkSize uint32
}

type segment struct {
	id                 SegmentID
	fd                 *os.File
	currentBlockNumber uint32
	currentBlockSize   uint32
	closed             bool
	cache              *lru.Cache[uint64, []byte]
}

type segmentReader struct {
	segment     *segment
	blockNumber uint32
	chunkOffset int64
}

func (seg *segment) Read(blockNumber uint32, chunkOffset int64) ([]byte, error) {
	value, _, err := seg.readInternal(blockNumber, chunkOffset)
	return value, err
}

func (seg *segment) readInternal(blockNumber uint32, chunkOffset int64) ([]byte, *ChunkPosition, error) {
	if seg.closed {
		return nil, nil, ErrClosed
	}

	var (
		result    []byte
		segSize   = seg.Size()
		nextChunk = &ChunkPosition{SegmentId: seg.id}
	)
	for {
		size := int64(blockSize)
		offset := int64(blockNumber * blockSize)
		if size+offset > segSize {
			size = segSize - offset
		}

		if chunkOffset >= size {
			return nil, nil, io.EOF
		}

		var block []byte
		var ok bool
		// 先尝试从缓存中读
		if seg.cache != nil {
			block, ok = seg.cache.Get(seg.getCacheKey(blockNumber))
		}

		if !ok || len(block) == 0 {
			block = make([]byte, size)
			_, err := seg.fd.ReadAt(block, offset)
			if err != nil {
				return nil, nil, err
			}
			// 将读出来的block放入到缓存中
			if seg.cache != nil && size == blockSize {
				seg.cache.Add(seg.getCacheKey(blockNumber), block)
			}
		}

		header := make([]byte, chunkHeaderSize)
		copy(header, block[chunkOffset:chunkOffset+chunkHeaderSize])

		// 长度
		length := binary.LittleEndian.Uint16(header[4:6])

		start := chunkOffset + chunkHeaderSize
		result = append(result, block[start:start+int64(length)]...)

		// 校验和
		checksumEnd := chunkOffset + chunkHeaderSize + int64(length)
		checksum := crc32.ChecksumIEEE(block[chunkOffset+4 : checksumEnd])
		savedSum := binary.LittleEndian.Uint32(header[:4])
		if savedSum != checksum {
			return nil, nil, ErrInvalidCRC
		}

		// type
		chunkType := header[6]
		if chunkType == ChunkTypeFull || chunkType == ChunkTypeLast {
			nextChunk.BlockNumber = blockNumber
			nextChunk.ChunkOffset = checksumEnd
			if checksumEnd+chunkHeaderSize >= blockSize {
				nextChunk.BlockNumber += 1
				nextChunk.ChunkOffset = 0
			}
			break
		}

		blockNumber += 1
		chunkOffset = 0

	}
	return result, nextChunk, nil
}

func (seg *segment) Size() int64 {
	return int64(seg.currentBlockNumber*blockSize + seg.currentBlockSize)
}

func (seg *segment) getCacheKey(blockNumber uint32) uint64 {
	return uint64(seg.id)<<32 | uint64(blockNumber)
}

func (seg *segment) Sync() error {
	if seg.closed {
		return nil
	}
	return seg.fd.Sync()
}

func (seg *segment) Write(data []byte) (*ChunkPosition, error) {
	if seg.closed {
		return nil, ErrClosed
	}

	if seg.currentBlockSize+chunkHeaderSize >= blockSize {
		if seg.currentBlockSize < blockSize {
			padding := make([]byte, blockSize-seg.currentBlockSize)
			if _, err := seg.fd.Write(padding); err != nil {
				return nil, err
			}
		}

		// A new block, clear the current block size.
		seg.currentBlockNumber += 1
		seg.currentBlockSize = 0
	}

	// the start position(for read operation)
	position := &ChunkPosition{
		SegmentId:   seg.id,
		BlockNumber: seg.currentBlockNumber,
		ChunkOffset: int64(seg.currentBlockSize),
	}
	dataSize := uint32(len(data))

	if seg.currentBlockSize+dataSize+chunkHeaderSize <= blockSize {
		err := seg.writeInternal(data, ChunkTypeFull)
		if err != nil {
			return nil, err
		}
		position.ChunkSize = dataSize + chunkHeaderSize
		return position, nil
	}

	var leftSize = dataSize
	var blockCount uint32 = 0
	for leftSize > 0 {
		chunkSize := blockSize - seg.currentBlockSize - chunkHeaderSize
		if chunkSize > leftSize {
			chunkSize = leftSize
		}
		chunk := make([]byte, chunkSize)

		var end = dataSize - leftSize + chunkSize
		if end > dataSize {
			end = dataSize
		}

		copy(chunk[:], data[dataSize-leftSize:end])

		var err error
		if leftSize == dataSize {
			// First Chunk
			err = seg.writeInternal(chunk, ChunkTypeFirst)
		} else if leftSize == chunkSize {
			// Last Chunk
			err = seg.writeInternal(chunk, ChunkTypeLast)
		} else {
			// Middle Chunk
			err = seg.writeInternal(chunk, ChunkTypeMiddle)
		}
		if err != nil {
			return nil, err
		}
		leftSize -= chunkSize
		blockCount += 1
	}

	position.ChunkSize = blockCount*chunkHeaderSize + dataSize
	return position, nil

}

func (seg *segment) writeInternal(data []byte, chunkType ChunkType) error {
	dataSize := uint32(len(data))
	buf := make([]byte, dataSize+chunkHeaderSize)

	// Length	2 Bytes	index:4-5
	binary.LittleEndian.PutUint16(buf[4:6], uint16(dataSize))
	// Type	1 Byte	index:6
	buf[6] = chunkType
	// data N Bytes index:7-end
	copy(buf[7:], data)
	// Checksum	4 Bytes index:0-3
	sum := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], sum)

	// append to the file
	if _, err := seg.fd.Write(buf); err != nil {
		return err
	}

	if seg.currentBlockSize > blockSize {
		panic("wrong! can not exceed the block size")
	}
	// update the corresponding fields
	seg.currentBlockSize += dataSize + chunkHeaderSize
	// A new block
	if seg.currentBlockSize == blockSize {
		seg.currentBlockNumber += 1
		seg.currentBlockSize = 0
	}
	return nil
}

func (seg *segment) NewReader() *segmentReader {
	return &segmentReader{
		segment:     seg,
		blockNumber: 0,
		chunkOffset: 0,
	}
}

func (segReader *segmentReader) Next() ([]byte, *ChunkPosition, error) {
	// The segment file is closed
	if segReader.segment.closed {
		return nil, nil, ErrClosed
	}
	// this position describes the current chunk info
	chunkPosition := &ChunkPosition{
		SegmentId:   segReader.segment.id,
		BlockNumber: segReader.blockNumber,
		ChunkOffset: segReader.chunkOffset,
	}
	value, nextChunk, err := segReader.segment.readInternal(
		segReader.blockNumber,
		segReader.chunkOffset,
	)
	if err != nil {
		return nil, nil, err
	}

	chunkPosition.ChunkSize =
		nextChunk.BlockNumber*blockSize + uint32(nextChunk.ChunkOffset) -
			(segReader.blockNumber*blockSize + uint32(segReader.chunkOffset))
	segReader.blockNumber = nextChunk.BlockNumber
	segReader.chunkOffset = nextChunk.ChunkOffset
	return value, chunkPosition, nil
}

func (seg *segment) Close() error {
	if seg.closed {
		return nil
	}

	seg.closed = true
	return seg.fd.Close()
}

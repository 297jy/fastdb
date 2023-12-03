package core

import (
	"errors"
	"fastdb"
	"fastdb/index"
	"fastdb/wal"
	"github.com/bwmarrin/snowflake"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	fileLockName       = "FLOCK"
	dataFileNameSuffix = ".SEG"
	hintFileNameSuffix = ".HINT"
	mergeFinNameSuffix = ".MERGEFIN"
)

// DB 代表 FastDB 的数据库实例
type DB struct {
	dataFiles *wal.WAL
	hintFile  *wal.WAL
	index     index.Indexer
	options   fastdb.DbOptions
	fileLock  *flock.Flock
	mu        sync.RWMutex
	closed    bool
	// mergeRunning 代表数据库正在被合并
	mergeRunning uint32
}

func Open(options fastdb.DbOptions) (*DB, error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// create data directory if not exist
	if _, err := os.Stat(options.DirPath); err != nil {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// create file lock, prevent multiple processes from using the same database directory
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, fastdb.ErrDatabaseIsUsing
	}

	walFiles, err := wal.Open(wal.Options{
		DirPath:        options.DirPath,
		SegmentSize:    options.SegmentSize,
		SegmentFileExt: dataFileNameSuffix,
		BlockCache:     options.BlockCache,
		Sync:           options.Sync,
		BytesPerSync:   options.BytesPerSync,
	})
	if err != nil {
		return nil, err
	}

	db := &DB{
		dataFiles: walFiles,
		index:     index.NewIndexer(),
		options:   options,
		fileLock:  fileLock,
	}
	if err = db.loadIndexFromWAL(); err != nil {
		return nil, err
	}

	return db, nil
}

func checkOptions(options fastdb.DbOptions) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.SegmentSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}

// loadIndexFromWAL 从 WAL 文件中，重新加载索引
func (db *DB) loadIndexFromWAL() error {
	indexRecords := make(map[uint64][]*IndexRecord)

	reader := db.dataFiles.NewReader()
	for {
		chunk, position, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		record := decodeLogRecord(chunk)

		if record.Type == LogRecordBatchFinished {
			batchId, err := snowflake.ParseBytes(record.Key)
			if err != nil {
				return err
			}
			for _, idxRecord := range indexRecords[uint64(batchId)] {
				if idxRecord.recordType == LogRecordNormal {
					db.index.Put(idxRecord.key, idxRecord.position)
				}
				if idxRecord.recordType == LogRecordDeleted {
					db.index.Delete(idxRecord.key)
				}
			}

			delete(indexRecords, uint64(batchId))
		} else {
			indexRecords[record.BatchId] = append(indexRecords[record.BatchId],
				&IndexRecord{
					key:        record.Key,
					recordType: record.Type,
					position:   position,
				})
		}
	}
	return nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.dataFiles.Close(); err != nil {
		return err
	}

	if err := db.fileLock.Unlock(); err != nil {
		return err
	}

	db.closed = true
	return nil
}

func (db *DB) Put(key []byte, value []byte) error {
	options := fastdb.DefaultBatchOptions
	options.Sync = false
	batch := db.NewBatch(options)
	if err := batch.Put(key, value); err != nil {
		return err
	}
	return batch.Commit()
}

func (db *DB) Get(key []byte) ([]byte, error) {
	options := fastdb.DefaultBatchOptions
	// Read-only operation
	options.ReadOnly = true
	batch := db.NewBatch(options)
	defer func() {
		_ = batch.Commit()
	}()
	return batch.Get(key)
}

func (db *DB) Delete(key []byte) error {
	options := fastdb.DefaultBatchOptions
	options.Sync = false
	batch := db.NewBatch(options)
	if err := batch.Delete(key); err != nil {
		return err
	}
	return batch.Commit()
}

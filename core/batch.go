package core

import (
	"fastdb"
	"fastdb/wal"
	"fmt"
	"github.com/bwmarrin/snowflake"
	"sync"
)

// Batch 是个对数据库的批量操作

type Batch struct {
	db            *DB
	pendingWrites map[string]*LogRecord
	options       fastdb.BatchOptions
	mu            sync.RWMutex
	committed     bool
	batchId       *snowflake.Node
}

func (db *DB) NewBatch(options fastdb.BatchOptions) *Batch {
	batch := &Batch{
		db:        db,
		options:   options,
		committed: false,
	}
	if !options.ReadOnly {
		batch.pendingWrites = make(map[string]*LogRecord)
		node, err := snowflake.NewNode(1)
		if err != nil {
			panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
		}
		batch.batchId = node
	}
	batch.lock()
	return batch
}

func (b *Batch) lock() {
	if b.options.ReadOnly {
		b.db.mu.RLock()
	} else {
		b.db.mu.Lock()
	}
}

func (b *Batch) unlock() {
	if b.options.ReadOnly {
		b.db.mu.RUnlock()
	} else {
		b.db.mu.Unlock()
	}
}

func (b *Batch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return fastdb.ErrKeyIsEmpty
	}
	if b.db.closed {
		return fastdb.ErrDBClosed
	}
	if b.options.ReadOnly {
		return fastdb.ErrReadOnlyBatch
	}

	b.mu.Lock()
	b.pendingWrites[string(key)] = &LogRecord{
		Key:   key,
		Value: value,
		Type:  LogRecordNormal,
	}
	b.mu.Unlock()
	return nil
}

func (b *Batch) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fastdb.ErrKeyIsEmpty
	}
	if b.db.closed {
		return nil, fastdb.ErrDBClosed
	}

	if b.pendingWrites != nil {
		b.mu.RLock()
		if record := b.pendingWrites[string(key)]; record != nil {
			if record.Type == LogRecordDeleted {
				b.mu.RUnlock()
				return nil, fastdb.ErrKeyNotFound
			}
			b.mu.RUnlock()
			return record.Value, nil
		}
	}

	chunkPosition := b.db.index.Get(key)
	if chunkPosition == nil {
		return nil, fastdb.ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(chunkPosition)
	if err != nil {
		return nil, err
	}

	record := decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted {
		return nil, fastdb.ErrKeyNotFound
	}
	return record.Value, nil
}

func (b *Batch) Delete(key []byte) error {
	if len(key) == 0 {
		return fastdb.ErrKeyIsEmpty
	}
	if b.db.closed {
		return fastdb.ErrDBClosed
	}
	if b.options.ReadOnly {
		return fastdb.ErrReadOnlyBatch
	}

	b.mu.Lock()
	if position := b.db.index.Get(key); position != nil {
		// write to pendingWrites if the key exists
		b.pendingWrites[string(key)] = &LogRecord{
			Key:  key,
			Type: LogRecordDeleted,
		}
	} else {
		delete(b.pendingWrites, string(key))
	}
	b.mu.Unlock()

	return nil
}

func (b *Batch) Commit() error {
	defer b.unlock()
	if b.db.closed {
		return fastdb.ErrDBClosed
	}

	if b.options.ReadOnly || len(b.pendingWrites) == 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.committed {
		return fastdb.ErrBatchCommitted
	}

	batchId := b.batchId.Generate()
	positions := make(map[string]*wal.ChunkPosition)

	for _, record := range b.pendingWrites {
		record.BatchId = uint64(batchId)
		encRecord := encodeLogRecord(record)
		pos, err := b.db.dataFiles.Write(encRecord)
		if err != nil {
			return err
		}
		positions[string(record.Key)] = pos
	}

	endRecord := encodeLogRecord(&LogRecord{
		Key:  batchId.Bytes(),
		Type: LogRecordBatchFinished,
	})
	if _, err := b.db.dataFiles.Write(endRecord); err != nil {
		return err
	}

	// flush wal if necessary
	if b.options.Sync && !b.db.options.Sync {
		if err := b.db.dataFiles.Sync(); err != nil {
			return err
		}
	}

	// 最后更新索引
	for key, record := range b.pendingWrites {
		if record.Type == LogRecordDeleted {
			b.db.index.Delete(record.Key)
		} else {
			b.db.index.Put(record.Key, positions[key])
		}
	}

	b.committed = true
	return nil
}

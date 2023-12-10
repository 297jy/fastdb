package core

import (
	"fastdb/common"
	"fastdb/config"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func destroyDB(db *DB) {
	_ = db.Close()
	_ = os.RemoveAll(db.options.DirPath)
}

func TestBatch_GET_Normal(t *testing.T) {
	options := config.DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	batch1 := db.NewBatch(config.DefaultBatchOptions)
	err = batch1.Put(common.GetTestKey(12), common.RandomValue(128))
	assert.Nil(t, err)
	val1, err := batch1.Get(common.GetTestKey(12))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	_ = batch1.Commit()

	generateData(t, db, 400, 500, 4*config.KB)
	batch2 := db.NewBatch(config.DefaultBatchOptions)
	err = batch2.Delete(common.GetTestKey(450))
	assert.Nil(t, err)
	val, err := batch2.Get(common.GetTestKey(450))
	assert.Nil(t, val)
	assert.Equal(t, common.ErrKeyNotFound, err)
	_ = batch2.Commit()

}

func TestBatch_LoadingIndex_Normal(t *testing.T) {
	data := generateDataMap(400, 500, 4*config.KB)
	options := config.DefaultOptions
	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)

	for k, v := range data {
		err := db.Put([]byte(k), []byte(v))
		assert.Nil(t, err)
	}

	_ = db.Close()
	db, err = Open(options)
	assert.Nil(t, err)
	for k, v := range data {
		res, _ := db.Get([]byte(k))
		assert.Equal(t, res, []byte(v))
	}
}

func generateData(t *testing.T, db *DB, start, end int, valueLen int) {
	for ; start < end; start++ {
		err := db.Put(common.GetTestKey(start), common.RandomValue(valueLen))
		assert.Nil(t, err)
	}
}

func generateDataMap(start, end, valueLen int) map[string]string {
	data := make(map[string]string)
	for ; start < end; start++ {
		key := string(common.GetTestKey(start))
		val := string(common.RandomValue(valueLen))
		data[key] = val
	}
	return data
}

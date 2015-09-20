package tsdb

import (
	"encoding/binary"
	"path/filepath"
	"sync"

	"github.com/gsdocker/gslogger"
	"github.com/syndtr/goleveldb/leveldb"
)

// _SEQIDGen the metadata storage database which is a wrapper of boltdb
type _SEQIDGen struct {
	sync.Mutex
	gslogger.Log
	db *leveldb.DB // bolt metadata db
}

func newSEQIDGen(dir string) (*_SEQIDGen, error) {

	db, err := leveldb.OpenFile(filepath.Join(dir, "id.db"), nil)

	if err != nil {
		return nil, err
	}

	return &_SEQIDGen{
		Log: gslogger.Get("tsdb-id"),
		db:  db,
	}, nil
}

// SQID generate new SQID for current key
func (gen *_SEQIDGen) SQID(key string) (uint64, error) {

	gen.Lock()
	defer gen.Unlock()

	buff, err := gen.db.Get([]byte(key), nil)

	if err != nil && leveldb.ErrNotFound != err {
		return 0, err
	}

	var id uint64

	if buff != nil {
		id = binary.BigEndian.Uint64(buff) + 1
	}

	buff = make([]byte, 64)

	return id, gen.db.Put([]byte(key), buff, nil)

}

func (gen *_SEQIDGen) Close() {
	gen.db.Close()
}

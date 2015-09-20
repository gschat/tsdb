package tsdb

import (
	"encoding/binary"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/gsdocker/gserrors"
	"github.com/gsdocker/gslogger"
)

// _SEQIDGen the metadata storage database which is a wrapper of boltdb
type _SEQIDGen struct {
	gslogger.Log
	db *bolt.DB // bolt metadata db
}

func newSEQIDGen(dir string) (*_SEQIDGen, error) {

	db, err := bolt.Open(filepath.Join(dir, "id.db"), 0600, nil)

	if err != nil {
		return nil, err
	}

	return &_SEQIDGen{
		Log: gslogger.Get("tsdb-id"),
		db:  db,
	}, nil
}

// SQID generate new SQID for current key
func (gen *_SEQIDGen) SQID(key string) (id uint64, err error) {

	err = gen.db.Update(func(tx *bolt.Tx) error {

		bucket, err := tx.CreateBucketIfNotExists([]byte(strSQID))
		if err != nil {
			return gserrors.Newf(err, "create bucket storage error")
		}

		val := bucket.Get([]byte(key))

		if val != nil {
			id = binary.BigEndian.Uint64(val) + 1
		}

		val = make([]byte, 64)

		binary.BigEndian.PutUint64(val, id)

		return bucket.Put([]byte(key), val)
	})

	return

}

func (gen *_SEQIDGen) Close() {
	gen.db.Close()
}

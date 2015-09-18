package tsdb

import (
	"encoding/binary"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/gsdocker/gserrors"
)

const (
	strSQID = "SQID"
)

// _MetaDB the metadata storage database which is a wrapper of boltdb
type _MetaDB struct {
	db *bolt.DB // bolt metadata db
}

func newMetaDB(dir string) (*_MetaDB, error) {

	db, err := bolt.Open(filepath.Join(dir, "metadata.db"), 0600, nil)

	if err != nil {
		return nil, err
	}

	return &_MetaDB{
		db: db,
	}, nil
}

// SQID generate new SQID for current key
func (metadb *_MetaDB) SQID(key string) (id uint64, err error) {

	err = metadb.db.Update(func(tx *bolt.Tx) error {

		bucket, err := tx.CreateBucketIfNotExists([]byte(strSQID))
		if err != nil {
			return gserrors.Newf(err, "create bucket storage error")
		}

		val := bucket.Get([]byte(key))

		if val != nil {
			id = binary.BigEndian.Uint64(val) + 1
		} else {
			val = make([]byte, 8)
		}

		binary.BigEndian.PutUint64(val, id)

		return bucket.Put([]byte(key), val)
	})

	return

}

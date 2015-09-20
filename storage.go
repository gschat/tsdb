package tsdb

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/gsdocker/gsconfig"
	"github.com/gsdocker/gserrors"
	"github.com/gsdocker/gslogger"
	"github.com/syndtr/goleveldb/leveldb"
)

type _Storage struct {
	gslogger.Log             // Mixin logger
	db           *leveldb.DB // bolt db
	key          string      // storage key
	valen        int         // value length
}

type _DB struct {
	gslogger.Log                    // Mixin logger
	sync.RWMutex                    // mixin mutex
	db           *leveldb.DB        // bolt  db
	storages     map[string]Storage // storages
	valen        int                /// max cached message
}

func newDB(dir string) (Persistence, error) {
	db, err := leveldb.OpenFile(filepath.Join(dir, "keyspace.db"), nil)

	if err != nil {
		return nil, err
	}

	return &_DB{
		Log:      gslogger.Get("tsdb.backend"),
		db:       db,
		storages: make(map[string]Storage),
		valen:    gsconfig.Int("tsdb.value.length", 1024),
	}, nil
}

func (db *_DB) Close() {
	db.db.Close()
}

func (db *_DB) Storage(key string) (Storage, error) {
	db.RLock()
	storage, ok := db.storages[key]
	db.RUnlock()

	if ok {
		return storage, nil
	}

	db.Lock()
	defer db.Unlock()

	db.storages[key] = db.newStorage(key)

	return db.storages[key], nil
}

func (db *_DB) newStorage(key string) Storage {
	return &_Storage{
		Log:   db.Log,
		db:    db.db,
		key:   key,
		valen: db.valen,
	}
}

func (storage *_Storage) Close() {

}
func (storage *_Storage) Write(val *DBValue) error {

	var buff bytes.Buffer

	err := WriteDBValue(&buff, val)

	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s:%d", storage.key, val.ID%uint64(storage.valen))

	return storage.db.Put([]byte(key), buff.Bytes(), nil)
}
func (storage *_Storage) Read(version uint64) (val *DBValue, ok bool) {

	key := fmt.Sprintf("%s:%d", storage.key, version%uint64(storage.valen))

	buff, err := storage.db.Get([]byte(key), nil)

	if err != nil {
		if err != leveldb.ErrNotFound {
			storage.E("get %s -> value \n%s", key, gserrors.Newf(err, ""))
		}
		
		return nil, false
	}

	if buff == nil {
		return nil, false
	}

	val, err = ReadDBValue(bytes.NewBuffer(buff))

	if err != nil {
		storage.E("unmarshal %s -> %s error\n%s", key, buff, err)
		return nil, false
	}

	return val, true
}

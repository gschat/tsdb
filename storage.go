package tsdb

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/gsdocker/gsconfig"
	"github.com/gsdocker/gserrors"
	"github.com/gsdocker/gslogger"
	"github.com/jmhodges/levigo"
)

type _DB struct {
	gslogger.Log                      // Mixin logger
	sync.Mutex                        // mixin mutex
	db           *levigo.DB           // bolt  db
	storages     map[string]Storage   // storages
	valen        int                  /// max cached message
	writeOpts    *levigo.WriteOptions // write options
	readOpts     *levigo.ReadOptions  // read options
}

func newDB(dir string) (Storage, error) {
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)

	db, err := levigo.Open(filepath.Join(dir, "keyspace.db"), opts)

	if err != nil {
		return nil, err
	}

	return &_DB{
		Log:       gslogger.Get("tsdb.backend"),
		db:        db,
		storages:  make(map[string]Storage),
		valen:     gsconfig.Int("tsdb.value.length", 1024),
		writeOpts: levigo.NewWriteOptions(),
		readOpts:  levigo.NewReadOptions(),
	}, nil
}

func (db *_DB) Close() {
	db.db.Close()
}

func (db *_DB) Write(name string, val *DBValue) error {

	var buff bytes.Buffer

	err := WriteDBValue(&buff, val)

	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s:%d", name, val.ID%uint64(db.valen))

	db.Lock()
	defer db.Unlock()

	return db.db.Put(db.writeOpts, []byte(key), buff.Bytes())
}

func (db *_DB) Read(name string, version uint64) (val *DBValue, ok bool) {

	key := fmt.Sprintf("%s:%d", name, version%uint64(db.valen))

	buff, err := db.db.Get(db.readOpts, []byte(key))

	if err != nil {
		db.E("get %s -> value \n%s", key, gserrors.Newf(err, ""))

		return nil, false
	}

	if buff == nil {
		return nil, false
	}

	val, err = ReadDBValue(bytes.NewBuffer(buff))

	if err != nil {
		db.E("unmarshal %s -> %s error\n%s", key, buff, err)
		return nil, false
	}

	return val, true
}

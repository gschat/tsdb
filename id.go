package tsdb

import (
	"encoding/binary"
	"path/filepath"
	"sync"

	"github.com/gsdocker/gslogger"
	"github.com/jmhodges/levigo"
)

// _SEQIDGen the metadata storage database which is a wrapper of boltdb
type _SEQIDGen struct {
	sync.Mutex
	gslogger.Log
	db        *levigo.DB           // bolt metadata db
	writeOpts *levigo.WriteOptions // write options
	readOpts  *levigo.ReadOptions  // read options
}

func newSEQIDGen(dir string) (*_SEQIDGen, error) {

	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)

	db, err := levigo.Open(filepath.Join(dir, "id.db"), opts)

	if err != nil {
		return nil, err
	}

	return &_SEQIDGen{
		Log:       gslogger.Get("tsdb-id"),
		db:        db,
		writeOpts: levigo.NewWriteOptions(),
		readOpts:  levigo.NewReadOptions(),
	}, nil
}

func (gen *_SEQIDGen) Current(key string) (uint64, bool) {
	gen.Lock()
	defer gen.Unlock()

	buff, err := gen.db.Get(gen.readOpts, []byte(key))

	if err != nil {
		return 0, false
	}

	if buff != nil {
		id := binary.BigEndian.Uint64(buff)
		return id, true
	}

	return 0, false
}

// SQID generate new SQID for current key
func (gen *_SEQIDGen) SQID(key string) (uint64, error) {

	gen.Lock()
	defer gen.Unlock()

	buff, err := gen.db.Get(gen.readOpts, []byte(key))

	if err != nil {
		return 0, err
	}

	var id uint64

	if buff != nil {
		id = binary.BigEndian.Uint64(buff) + 1
	}

	buff = make([]byte, 64)

	return id, gen.db.Put(gen.writeOpts, []byte(key), buff)

}

func (gen *_SEQIDGen) Close() {
	gen.db.Close()
}

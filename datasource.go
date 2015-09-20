package tsdb

import (
	"os"
	"sync"

	"github.com/gsdocker/gsconfig"
	"github.com/gsdocker/gslogger"
	"github.com/gsdocker/gsos/fs"
)

type _DataSource struct {
	gslogger.Log                     // Mixin Log
	sync.RWMutex                     // Mixin mutex
	SEQIDGen                         // Mixin seq id gen service
	Persistence                      // Mixin persistence service
	cached       map[string]*_Cached // L1 cached
	cachedsize   int                 // cache size
}

// Open create new tsdb with root directory
func Open(filepath string) (DataSource, error) {

	if !fs.Exists(filepath) {
		err := os.MkdirAll(filepath, 0644)
		if err != nil {
			return nil, err
		}
	}

	idgen, err := newSEQIDGen(filepath)

	if err != nil {
		return nil, err
	}

	persistence, err := newDB(filepath)

	if err != nil {
		return nil, err
	}

	return &_DataSource{
		Log:         gslogger.Get("tsdb"),
		SEQIDGen:    idgen,
		Persistence: persistence,
		cached:      make(map[string]*_Cached),
		cachedsize:  gsconfig.Int("tsdb.cached.size", 1024),
	}, nil
}

func (datasource *_DataSource) Update(key string, data []byte) error {
	storage, err := datasource.Storage(key)

	if err != nil {
		return nil
	}

	id, err := datasource.SQID(key)

	if err != nil {
		return err
	}

	val := &DBValue{id, data}

	err = storage.Write(val)

	if err != nil {
		return err
	}

	datasource.RLock()
	defer datasource.RUnlock()

	if cached, ok := datasource.cached[key]; ok {
		cached.Update(val)
	}

	return nil
}

func (datasource *_DataSource) Query(key string, version uint64) (DataSet, error) {

	storage, err := datasource.Storage(key)

	if err != nil {
		return nil, nil
	}

	datasource.Lock()
	defer datasource.Unlock()

	cached, ok := datasource.cached[key]

	if !ok {

		cached = newCached(datasource.cachedsize)

		datasource.cached[key] = cached
	}

	return datasource.makeDataSet(storage, cached, version), nil
}

func (datasource *_DataSource) Close() {
	datasource.SEQIDGen.Close()
	datasource.Persistence.Close()
}

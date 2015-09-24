package tsdb

import (
	"errors"
	"time"

	"github.com/gsdocker/gsconfig"
	"github.com/gsdocker/gserrors"
	"github.com/gsdocker/gslogger"
)

// Errors
var (
	ErrOp = errors.New("unsupport operation")
)

type _DataSet struct {
	gslogger.Log               // mixin logger
	storage      Storage       // storage
	cached       *_Cached      // cached
	miniVersion  uint64        // mini version of this dataset
	cursor       uint64        // current read cursor
	Q            chan *DBValue // dataset queue
	closed       chan bool     // closed flag
	key          string        // key string
}

func (datasource *_DataSource) makeDataSet(key string, storage Storage, cached *_Cached, miniVersion uint64) DataSet {
	dataset := &_DataSet{
		Log:         gslogger.Get("dataset"),
		storage:     storage,
		cached:      cached,
		miniVersion: miniVersion,
		cursor:      miniVersion,
		Q:           make(chan *DBValue),
		closed:      make(chan bool),
		key:         key,
	}

	go dataset.readLoop()

	return dataset
}

func (dataset *_DataSet) readLoop() {

	timeout := gsconfig.Seconds("tsdb.dataset.sleep", 2)

	timer := time.NewTimer(timeout)

	defer func() {
		if e := recover(); e != nil {
		}

		timer.Stop()
	}()

	for {

		val, ok := dataset.cached.Get(dataset.cursor)

		if !ok {

			dataset.D("read data [%s %d]", dataset.key, dataset.cursor)
			val, ok = dataset.storage.Read(dataset.key, dataset.cursor)
			dataset.D("read data [%s %d] -- success", dataset.key, dataset.cursor)

			if !ok {
				timer.Reset(timeout)
				select {
				case <-timer.C:
					continue
				case <-dataset.closed:
					return
				}
			}
		}

		select {
		case <-dataset.closed:
			return
		case dataset.Q <- val:
			dataset.cursor++
		}
	}
}

func (dataset *_DataSet) MiniVersion() uint64 {
	return dataset.miniVersion
}

func (dataset *_DataSet) Stream() <-chan *DBValue {
	return dataset.Q
}

func (dataset *_DataSet) Next() (data []byte, version uint64) {

	defer func() {
		if e := recover(); e != nil {
		}
	}()

	select {
	case <-dataset.closed:
		gserrors.Panicf(ErrOp, "call Next on closed dataset")
		return nil, 0
	case val := <-dataset.Q:
		return val.Content, val.ID
	}
}

func (dataset *_DataSet) Close() {
	close(dataset.closed)
	close(dataset.Q)
}

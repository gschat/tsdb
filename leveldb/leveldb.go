package main

import (
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/gsdocker/gslogger"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	dbname = "./test"
)

var db *leveldb.DB

func init() {
	err := os.RemoveAll(dbname)

	if err != nil {
		panic(err)
	}

	db, err = leveldb.OpenFile(dbname, nil)

	if err != nil {
		panic(err)
	}

}

var c = flag.Int("c", 20000, "simulator clients")

var t = flag.Duration("t", 10*time.Millisecond, "update timeout")

var m = flag.String("m", "hello world", "update messages")

var counter uint64

var timestamp = time.Now()

func main() {
	log := gslogger.Get("bench")

	for i := 0; i < *c; i++ {
		key := fmt.Sprintf("key%d", i)
		go func() {
			for _ = range time.Tick(*t) {
				err := db.Put([]byte(key), []byte(*m), nil)

				atomic.AddUint64(&counter, 1)

				if err != nil {
					log.E("update err :%s", err)
				}
			}
		}()
	}

	for _ = range time.Tick(2 * time.Second) {
		log.I("update %d ops/s", counter/2)
		atomic.StoreUint64(&counter, 0)
	}
}

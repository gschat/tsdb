package main

import (
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/gsdocker/gslogger"
	"github.com/jmhodges/levigo"
)

const (
	dbname = "./test"
)

var db *levigo.DB

func init() {
	err := os.RemoveAll(dbname)

	if err != nil {
		panic(err)
	}

	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)

	db, err = levigo.Open(dbname, opts)

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

	flag.Parse()

	log := gslogger.Get("bench")

	for i := 0; i < *c; i++ {
		key := fmt.Sprintf("key%d", i)
		go func() {
			wo := levigo.NewWriteOptions()

			for _ = range time.Tick(*t) {
				err := db.Put(wo, []byte(key), []byte(*m))

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

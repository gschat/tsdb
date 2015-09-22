package main

import (
	"flag"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gsdocker/gslogger"
	"github.com/ssdb/gossdb/ssdb"
)

const (
	dbname = "./test"
)

var c = flag.Int("c", 20000, "simulator clients")

var t = flag.Duration("t", 10*time.Millisecond, "update timeout")

var m = flag.String("m", "hello world", "update messages")

var counter uint64

var timestamp = time.Now()

func main() {
	log := gslogger.Get("bench")
	ip := "127.0.0.1"
	port := 8888
	db, err := ssdb.Connect(ip, port)
	if err != nil {
		log.E("connect ssdb error :%s", err)
	}

	for i := 0; i < *c; i++ {
		key := fmt.Sprintf("key%d", i)
		go func() {
			for _ = range time.Tick(*t) {
				_, err := db.Set(key, *m)

				if err != nil {
					log.E("update err :%s", err)
				}

				atomic.AddUint64(&counter, 1)
			}
		}()
	}

	for _ = range time.Tick(2 * time.Second) {
		log.I("update %d ops/s", counter/2)
		atomic.StoreUint64(&counter, 0)
	}
}

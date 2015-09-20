package test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gschat/tsdb"
)

var db tsdb.DataSource

const (
	dbname = "./tsdb"
)

func init() {

	err := os.RemoveAll(dbname)

	if err != nil {
		panic(err)
	}

	db, err = tsdb.Open(dbname)

	if err != nil {
		panic(err)
	}

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("tsdb.%d", i)
		go func() {

			for _ = range time.Tick(10 * time.Second) {
				db.Update(key, []byte(key))
			}

		}()
	}
}

func TestCreateKey(t *testing.T) {
	err := db.Update("tsdb.-1", []byte("hello world"))

	if err != nil {
		t.Fatal(err)
	}

	dataset, err := db.Query("tsdb.-1", 0)

	if err != nil {
		t.Fatal(err)
	}

	defer dataset.Close()

	data, version := dataset.Next()

	if string(data) != "hello world" {
		t.Fatal("check key value error")
	}

	if version != 0 {
		t.Fatal("check key version error")
	}

	err = db.Update("tsdb.-1", []byte("hello world"))

	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkUpdate(t *testing.B) {
	for i := 0; i < t.N; i++ {
		err := db.Update("tsdb.-1", []byte("hello world"))

		if err != nil {
			t.Fatal(err)
		}
	}
}

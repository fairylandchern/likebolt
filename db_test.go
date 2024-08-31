package likebolt

import (
	"fmt"
	"testing"
)

func TestDB_Put(t *testing.T) {
	db := InitDB("./db.txt")
	if db == nil {
		panic("err db nil")
	}
	err := db.Put([]byte("test"), []byte("123yuguangxing"))
	if err != nil {
		fmt.Println("err put data:", err)
		return
	}
	val := db.Get([]byte("test"))
	fmt.Println("get data:", string(val))
	err = db.Commit()
	if err != nil {
		fmt.Println("err commit:", err)
	}
}

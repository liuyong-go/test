package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/davecgh/go-spew/spew"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	db, err := sql.Open("mysql",
		"root:123@tcp(127.0.0.1:3306)/test")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxIdleConns(20)
	db.SetMaxOpenConns(30)
	for i := 0; i < 200; i++ {
		go db.Ping()
	}

	err = db.Ping()
	if err != nil {
		fmt.Println(err)
	}
	time.Sleep(2 * time.Second)
	spew.Dump(db.Stats())

}

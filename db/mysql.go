package db

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

func conn(sh string) (db *sql.DB) {
	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:3308)/"+sh)

	// if there is an error opening the connection, handle it
	if err != nil {
		panic(err.Error())
	}
	return db
}

// Execute `DESC schema.table`
// sh - schema, tb - table
func Desc(sh, tb string) {
	db := conn(sh)
	defer db.Close()

	desc, err := db.Query("DESC " + sh + "." + tb)
	fmt.Print(desc)
	if err != nil {
		panic(err.Error())
	}
}

func AlterComment(sh, tb, cmt string) {
	db := conn(sh)
	defer db.Close()

	_, err := db.Query("ALTER TABLE " + sh + "." + tb + " COMMENT = '" + cmt + "'")
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("OK")
}

func ShowTables(sh string) {
	db := conn(sh)
	defer db.Close()

	tbls, _ := db.Query("SHOW TABLES")
	var table string
	//var desc Table
	var count int
	for tbls.Next() {
		_ = tbls.Scan(&table)
		//rows, _ := db.Query("DESC " + sh + "." + table)
		fmt.Println(table)
		count++
	}
	fmt.Println("[", sh, "] Count :", count)
}

type Table struct {
	comment string
	name    string
}

package db

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/msklnko/kitana/util"
	"os"
	"text/tabwriter"
)

func conn(sh string) (db *sql.DB) {
	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:3308)/"+sh)

	// if there is an error opening the connection, handle it
	if err != nil {
		panic(err.Error())
	}
	return db
}

// Execute `ALTER COMMENT schema.table`
func AlterComment(sh, tb, cmt string) {
	db := conn(sh)
	defer db.Close()

	_, err := db.Query("alter table " + sh + "." + tb + " comment = '" + cmt + "'")
	util.Er(err)
}

// Execute `SHOW CREATE TABLE schema.table`
func ShowCreateTable(sh, tb string) {
	db := conn(sh)
	defer db.Close()

	desc, err := db.Query("show create table " + sh + "." + tb)
	util.Er(err)

	for desc.Next() {
		var (
			name string
			dsc  string
		)
		err = desc.Scan(&name, &dsc)
		util.Er(err)
		fmt.Println("Table: " + name)
		fmt.Println("Description: " + dsc)
	}
}

func ShowTables(sh string) {
	db := conn(sh)
	defer db.Close()

	tbls, _ := db.Query("show tables")
	var table string
	//var desc Table
	var count int
	for tbls.Next() {
		_ = tbls.Scan(&table)
		fmt.Println(table)
		count++
	}
	fmt.Println("[", sh, "] Count :", count)
}

func InformSchema(sh, tb string) {
	db := conn(sh)
	defer db.Close()

	rows, err := db.Query("select " +
		"partition_name, " +
		"partition_expression, " +
		"table_rows, " +
		"create_time, " +
		"partition_description " +
		"from information_schema.partitions " +
		"where table_name='" + tb + "' and table_schema= '" + sh + "'")
	util.Er(err)

	// Format in tab-separated columns with a tab stop of 8.

	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 0, '\t', 0)
	_, _ = fmt.Fprintln(w, "Name\tExpression\tRows\tCreatedAt\tTill\t")
	for rows.Next() {
		var (
			name  string
			expr  string
			count int
			cr    string
			desc  string
		)
		err := rows.Scan(&name, &expr, &count, &cr, &desc)
		util.Er(err)
		_, _ = fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\n", name, expr, count, cr, desc)
	}
	_ = w.Flush()
}

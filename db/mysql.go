package db

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/msklnko/kitana/util"
	"os"
	"text/tabwriter"
	"time"
)

func conn(sh string) (db *sql.DB) {
	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:3308)/"+sh)

	// if there is an error opening the connection, handle it
	if err != nil {
		panic(err.Error())
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)
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

func CheckTablePresent(sh, tb string) bool {
	db := conn(sh)
	defer db.Close()

	var res sql.NullInt32
	err := db.QueryRow("select 1 from information_schema.tables " +
		"where table_schema = '" + sh + "' and table_name = '" + tb + "'").Scan(&res)
	util.Er(err)

	return res.Valid
}

func InformSchema(sh, tb string) {
	db := conn(sh)
	defer db.Close()

	// Query
	rows, err := db.Query("select " +
		"partition_name, " +
		"partition_expression, " +
		"table_rows, " +
		"create_time, " +
		"partition_description " +
		"from information_schema.partitions " +
		"where table_name='" + tb + "' and table_schema= '" + sh + "'")
	util.Er(err)

	// Parse
	type row struct {
		name  sql.NullString
		expr  sql.NullString
		count sql.NullInt64
		cr    sql.NullString
		desc  sql.NullString
	}
	var parsed []row
	for rows.Next() {
		var r row
		err := rows.Scan(&r.name, &r.expr, &r.count, &r.cr, &r.desc)
		util.Er(err)
		parsed = append(parsed, r)
	}

	// Print
	if len(parsed) > 0 {
		// Format in tab-separated columns with a tab stop of 8.
		w := new(tabwriter.Writer)
		w.Init(os.Stdout, 0, 8, 0, '\t', 0)
		_, _ = fmt.Fprintln(w, "Name\tExpression\tRows\tCreatedAt\tTill\t")

		for _, s := range parsed {
			_, _ = fmt.Fprintf(w,
				"%s\t%s\t%d\t%s\t%s\n",
				s.name.String, s.expr.String, s.count.Int64, s.cr.String, s.desc.String)
		}
		_ = w.Flush()
	} else {
		fmt.Printf("Table '%s' doesn't exist\n", sh+"."+tb)
	}
}

func Partition(sh, tb, name, limiter string) {
	db := conn(sh)
	_, err := db.Query("alter table " + sh + "." + tb +
		" add partition (partition " + name + " values less than (" + limiter + "))")
	util.Er(err)
}

func DropPartition(sh, tb, partition string) {
	db := conn(sh)
	_, err := db.Query("alter table " + sh + "." + tb + " drop partition " + partition)
	util.Er(err)
}

package db

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/msklnko/kitana/cmt"
	"github.com/msklnko/kitana/util"
	"text/tabwriter"
	"time"
)

var db *sql.DB

func init() {
	d, err := sql.Open("mysql",
		util.Configuration.Database.Username+":"+util.Configuration.Database.Password+
			"@tcp("+util.Configuration.Database.Host+":"+util.Configuration.Database.Port+")/")
	if err != nil {
		util.Er(err)
	}
	db = d
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(2 * time.Minute)
}

// AlterComment Execute `ALTER COMMENT schema.table`
func AlterComment(sh, tb, cmt string) {
	_, err := db.Query("alter table " + sh + "." + tb + " comment = '" + cmt + "'")
	util.Er(err)
}

// ShowCreateTable Execute `SHOW CREATE TABLE schema.table`
func ShowCreateTable(sh, tb string) {
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

// ShowTables Show tables for db schema
func ShowTables(sh string, comment, part, def bool) {
	var query = "select table_name, table_comment from information_schema.tables where table_schema=\"" + sh + "\""
	if comment {
		query = query + " and table_comment !=''"
	} else if part {
		query = query + " and table_comment like '%" + util.PartIdentification + "%'"
	}

	tbls, err := db.Query(query)
	util.Er(err)

	//var desc Table
	var count int
	type row struct {
		name    string
		comment sql.NullString
	}
	var parsed []row
	for tbls.Next() {
		var r row
		err := tbls.Scan(&r.name, &r.comment)
		util.Er(err)
		parsed = append(parsed, r)
		count++
	}

	// Print
	if len(parsed) > 0 {
		util.Print(util.Ternar(def, "Name\tComment\tDefinition\t", "Name\tComment\t"),
			func(w *tabwriter.Writer) {
				for _, s := range parsed {
					if def {
						_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", s.name, s.comment.String, cmt.Def(s.comment.String))
					} else {
						_, _ = fmt.Fprintf(w, "%s\t%s\n", s.name, s.comment.String)
					}
				}
			})
		fmt.Println("[", sh, "] Count :", count)
	}
}

// CheckTablePresent Check provided table is present
func CheckTablePresent(sh, tb string) bool {
	var res sql.NullInt32
	err := db.QueryRow("select 1 from information_schema.tables " +
		"where table_schema = '" + sh + "' and table_name = '" + tb + "'").Scan(&res)
	util.Er(err)

	return res.Valid
}

// InformSchema Show info about partitions (for not partitioned table just count of rows)
func InformSchema(sh, tb string) {
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
		util.Print(
			"Name\tExpression\tRows\tCreatedAt\tTill\t",
			func(w *tabwriter.Writer) {
				for _, s := range parsed {
					_, _ = fmt.Fprintf(w,
						"%s\t%s\t%d\t%s\t%s\n",
						s.name.String, s.expr.String, s.count.Int64, s.cr.String, s.desc.String)
				}
			})
	} else {
		fmt.Printf("Table '%s' doesn't exist\n", sh+"."+tb)
	}
}

// AddPartition Add partition
func AddPartition(sh, tb, name, limiter string) {
	_, err := db.Query("alter table " + sh + "." + tb +
		" add partition (partition " + name + " values less than (" + limiter + "))")
	util.Er(err)
}

// DropPartition Drop partition(s) by name
func DropPartition(sh, tb, partition string) {
	_, err := db.Query("alter table " + sh + "." + tb + " drop partition " + partition)
	util.Er(err)
}

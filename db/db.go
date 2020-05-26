package db

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/msklnko/kitana/cmt"
	"github.com/msklnko/kitana/util"
	"strconv"
	"strings"
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
		query = query + " and table_comment like '%" + cmt.PartIdentification + "%'"
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
		util.Print(util.Ternary(def, "Name\tComment\tDefinition\t", "Name\tComment\t"),
			func(w *tabwriter.Writer) {
				for _, s := range parsed {
					if def {
						_, def := cmt.Def(s.comment.String)
						_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", s.name, s.comment.String, def)
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

// TODO ask
type Partition struct {
	Name  string
	Expr  string
	Count int64
	Cr    string
	Desc  int64
}

// InformSchema Shows info about partitions, bool flag identifies table doesn't partitioned or does not exist at all
func InformSchema(sh, tb string) ([]Partition, bool, string) {
	rows, err := db.Query("select " +
		"create_options, " +
		"table_comment, " +
		"p.partition_name, " +
		"p.partition_expression, " +
		"p.table_rows, " +
		"p.create_time, " +
		"p.partition_description " +
		"from information_schema.tables t join information_schema.partitions p on p.table_name = t.table_name " +
		"where t.table_name='" + tb + "' and t.table_schema= '" + sh + "'")
	util.Er(err)

	// Parse
	type row struct {
		status  sql.NullString
		comment sql.NullString
		name    sql.NullString
		expr    sql.NullString
		count   sql.NullInt64
		cr      sql.NullString
		desc    sql.NullInt64
	}
	var parsed []row
	for rows.Next() {
		var r row
		err := rows.Scan(&r.status, &r.comment, &r.name, &r.expr, &r.count, &r.cr, &r.desc)
		util.Er(err)
		parsed = append(parsed, r)
	}

	// Table does not exist
	if len(parsed) == 0 {
		return []Partition{}, false, ""
	}

	// Table exist but not partitioned
	if len(parsed) == 1 && parsed[0].status.String != "partitioned" {
		return []Partition{}, true, parsed[0].comment.String
	}

	s := make([]Partition, len(parsed))
	for i := 0; i < len(parsed); i++ {
		r := parsed[i]
		s[i] = Partition{Name: r.name.String, Expr: r.expr.String, Count: r.count.Int64,
			Cr: r.cr.String, Desc: r.desc.Int64}
	}
	return s, true, parsed[0].comment.String
}

// AddPartition Add partition
func AddPartition(sh, tb, name, limiter string) {
	_, err := db.Query("alter table " + sh + "." + tb +
		" add partition (partition " + name + " values less than (" + limiter + "))")
	util.Er(err)
}

// AddPartitions Add partitions to existing partitioned table
func AddPartitions(sh, tb string, partitions map[string]int64) {
	if len(partitions) == 0 {
		//Nothing to alter
		return
	}

	// Build sql for each partition
	var ps []string
	for n, l := range partitions {
		ps = append(ps, " partition "+n+" values less than ("+strconv.FormatInt(l, 10)+") ")
	}

	// Alter
	_, err := db.Query("alter table " + sh + "." + tb + " add partition (" + strings.Join(ps[:], ",") + ")")
	util.Er(err)
}

// DropPartition Drop partition(s) by name
func DropPartition(sh, tb, partition string) {
	_, err := db.Query("alter table " + sh + "." + tb + " drop partition " + partition)
	util.Er(err)
}

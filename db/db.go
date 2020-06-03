package db

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mono83/xray"
	"github.com/mono83/xray/args"
	"github.com/msklnko/kitana/config"
	"github.com/msklnko/kitana/definition"
	"github.com/msklnko/kitana/util"
)

func connect() (*sql.DB, error) {
	db, err := sql.Open("mysql", config.Configuration.MySQL().FormatDSN())
	defer db.Close()
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

// AlterComment Execute `ALTER COMMENT schema.table`
func AlterComment(database, table, comment string) error {
	db, er := connect()
	if er != nil {
		panic(er)
	}

	_, err := db.Exec(fmt.Sprintf(
		`ALTER TABLE %s.%s COMMENT='%s'`,
		database, table, comment,
	))
	if err != nil {
		xray.ROOT.Fork().Alert("Error adding comment to :name - :err", args.Name(table), args.Error{Err: err})
		return err
	}

	return nil
}

// ShowCreateTable Execute `databaseOW CREATE TABLE schema.table`
func ShowCreateTable(database, table string) {
	db, er := connect()
	if er != nil {
		panic(er)
	}

	desc, err := db.Query("database show create table " + database + "." + table)
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
func ShowTables(database string, comment, part, def bool) {
	var query = "select table_name, table_comment from information_schema.tables where table_schema=\"" + sh + "\""
	if comment {
		query = query + " and table_comment !=''"
	} else if part {
		query = query + " and table_comment like '%" + definition.PartIdentification + "%'"
	}

	db, er := connect()
	if er != nil {
		panic(er)
	}

	tablels, err := db.Query(query)
	util.Er(err)

	//var desc Table
	var count int
	type row struct {
		name    string
		comment sql.NullString
	}
	var parsed []row
	for tablels.Next() {
		var r row
		err := tablels.Scan(&r.name, &r.comment)
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
						_, def := definition.Parse(s.comment.String)
						_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", s.name, s.comment.String, def)
					} else {
						_, _ = fmt.Fprintf(w, "%s\t%s\n", s.name, s.comment.String)
					}
				}
			})
		fmt.Println("[", database, "] Count :", count)
	}
}

// CheckTablePresent Check provided table is present
func CheckTablePresent(database, table string) bool {
	db, er := connect()
	if er != nil {
		panic(er)
	}

	var res sql.NullInt32
	err := db.QueryRow("select 1 from information_schema.tables " +
		"where table_schema = '" + database + "' and table_name = '" + table + "'").Scan(&res)
	util.Er(err)

	return res.Valid
}

type Partition struct {
	Name       string
	Expression string
	Count      int64
	CreatedAt  string
	Limiter    int64
}

// InformSchema database rows info about partitions, bool flag identifies table doesn't partitioned or does not exist at all
func InformSchema(database, table string) ([]Partition, bool, string, error) {
	db, er := connect()
	if er != nil {
		panic(er)
	}

	rows, err := db.Query("select " +
		"create_options, " +
		"table_comment, " +
		"p.partition_name, " +
		"p.partition_expression, " +
		"p.table_rows, " +
		"p.create_time, " +
		"p.partition_description " +
		"from information_schema.tables t join information_schema.partitions p on p.table_name = t.table_name " +
		"where t.table_name='" + table + "' and t.table_schema= '" + database + "'")
	if err != nil {
		return nil, false, "", err
	}

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
		if err != nil {
			return nil, false, "", err
		}
		parsed = append(parsed, r)
	}

	// Table does not exist
	if len(parsed) == 0 {
		return []Partition{}, false, "", nil
	}

	// Table exist but not partitioned
	if len(parsed) == 1 && parsed[0].status.String != "partitioned" {
		return []Partition{}, true, parsed[0].comment.String, nil
	}

	s := make([]Partition, len(parsed))
	for i := 0; i < len(parsed); i++ {
		r := parsed[i]
		s[i] = Partition{Name: r.name.String, Expression: r.expr.String, Count: r.count.Int64,
			CreatedAt: r.cr.String, Limiter: r.desc.Int64}
	}
	return s, true, parsed[0].comment.String, nil
}

// AddPartitions Add partitions to existing partitioned table
func AddPartitions(database, table string, partitions map[string]int64) error {
	if len(partitions) == 0 {
		//Nothing to alter
		return nil
	}

	// Build sql for each partition
	var ps []string
	for n, l := range partitions {
		ps = append(ps, " partition "+n+" values less than ("+strconv.FormatInt(l, 10)+") ")
	}

	db, er := connect()
	if er != nil {
		panic(er)
	}

	// Alter
	_, err := db.Query(fmt.Sprintf(
		`alter table %s.%s  add partition ( %s )`,
		database, table, strings.Join(ps[:], ","),
	))

	return err
}

// DropPartition Drop partition(s) by name
func DropPartition(database, table string, partitions []string) error {
	db, er := connect()
	if er != nil {
		panic(er)
	}

	_, err := db.Query(fmt.Sprintf(
		`alter table %s.%s  drop partition %s`,
		database, table, strings.Join(partitions, ","),
	))

	return err
}

// CreateTableDuplicate Create duplicate from table without partitions
func CreateTableDuplicate(database, table, duplicateTable string) error {
	db, err := connect()
	if err != nil {
		panic(err)
	}

	// Create duplicate table
	_, err = db.Exec(fmt.Sprintf(
		`create table %s.%s LIKE %s.%s`,
		database, duplicateTable, database, table,
	))
	if err != nil {
		return err
	}

	// Remove partitions
	_, err = db.Exec(fmt.Sprintf(
		`alter table %s.%s remove partitioning`,
		database, duplicateTable,
	))

	return err
}

// ExchangePartition Copy partition data to another table
func ExchangePartition(database, table, duplicateTable, name string) error {
	db, err := connect()
	if err != nil {
		panic(err)
	}

	// Copy partition
	_, err = db.Exec(fmt.Sprintf(
		`alter table %s.%s exchange partition '%s' with table %s.%s`,
		database, table, name, database, duplicateTable,
	))

	return err
}

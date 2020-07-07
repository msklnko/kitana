package db

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mono83/xray"
	"github.com/mono83/xray/args"
	"github.com/msklnko/kitana/config"
	"github.com/msklnko/kitana/definition"
	"github.com/msklnko/kitana/util"
)

var db *sql.DB = nil

func connect() (*sql.DB, error) {
	if db == nil {
		conn, err := sql.Open("mysql", config.Configuration.MySQL().FormatDSN())
		if err != nil {
			return nil, err
		}
		if err := conn.Ping(); err != nil {
			return nil, err
		}
		conn.SetConnMaxLifetime(time.Second * 30)
		db = conn
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
func ShowCreateTable(database, table string) error {
	db, er := connect()
	if er != nil {
		panic(er)
	}

	desc, err := db.Query("show create table " + database + "." + table)
	if err != nil {
		return err
	}

	for desc.Next() {
		var (
			name string
			dsc  string
		)
		err = desc.Scan(&name, &dsc)
		if err != nil {
			return err
		}

		fmt.Println("Table: " + name)
		fmt.Println("Description: " + dsc)
	}
	return nil
}

// Table Description
type Table struct {
	Database string
	Name     string
	Comment  string
}

// ShowTables Show tables for db schema
func ShowTables(database string, comment, part bool) ([]Table, error) {
	var query = "show table status "

	// Collect conditions
	var where []string
	if len(database) > 0 {
		query = query + fmt.Sprintf("from %s", database)
	}
	if part {
		where = append(where, "`comment` like '%"+definition.PartitionIdentifier+"%'")
	} else if comment {
		where = append(where, "`comment` !=''")
	}

	if len(where) > 0 {
		query = query + " where "
		for i, condition := range where {
			if i != 0 {
				query = query + " and "
			}
			query = query + condition
		}
	}

	db, er := connect()
	if er != nil {
		panic(er)
	}

	tables, err := db.Query(query)
	if err != nil {
		return nil, err
	}

	columns, err := tables.Columns()
	if err != nil {
		return nil, err
	}

	nameIndex := util.IndexOf("Name", columns)
	commentIndex := util.IndexOf("Comment", columns)

	var count int

	var rows [][]interface{}
	for tables.Next() {
		row := make([]interface{}, len(columns))
		for i, _ := range columns {
			row[i] = new(sql.NullString)
		}
		if err := tables.Scan(row...); err != nil {
			return nil, err
		}
		rows = append(rows, row)
		count++
	}

	result := make([]Table, count)
	for i := 0; i < len(rows); i++ {
		r := rows[i]

		result[i] = Table{
			Database: database,
			Name:     r[nameIndex].(*sql.NullString).String,
			Comment:  r[commentIndex].(*sql.NullString).String,
		}
	}
	return result, nil
}

// CheckTablePresent Check provided table is present
func CheckTablePresent(database, table string) (bool, error) {
	db, er := connect()
	if er != nil {
		panic(er)
	}

	var res sql.NullInt32
	err := db.QueryRow("select 1 from information_schema.tables " +
		"where table_schema = '" + database + "' and table_name = '" + table + "'").Scan(&res)
	if err != nil {
		return false, err
	}

	return res.Valid, nil
}

// Partition Structure to represent partition
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
		s[i] = Partition{
			Name:       r.name.String,
			Expression: r.expr.String,
			Count:      r.count.Int64,
			CreatedAt:  r.cr.String,
			Limiter:    r.desc.Int64,
		}
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
	if err != nil {
		return err
	}

	// Change comment
	_, err = db.Exec(fmt.Sprintf(
		`alter table %s.%s comment 'Backup for %s'`,
		database, duplicateTable, table,
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
		`alter table %s.%s exchange partition %s with table %s.%s`,
		database, table, name, database, duplicateTable,
	))

	return err
}

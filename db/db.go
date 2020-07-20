package db

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/msklnko/kitana/definition"

	"github.com/mono83/xray"
	"github.com/mono83/xray/args"
	"github.com/msklnko/kitana/config"
	"github.com/msklnko/kitana/util"
)

var db *sql.DB = nil
var showTablePattern = regexp.MustCompile(`(?s)COMMENT='(\[GM:.*])'|PARTITION BY RANGE \(.(\w+)|PARTITION (\w+) VALUES LESS THAN \((\d+)`)

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
	db, err := connect()
	if err != nil {
		return err
	}

	var query = fmt.Sprintf(
		`ALTER TABLE %s.%s COMMENT='%s'`,
		database, table, comment,
	)
	xray.ROOT.Fork().Trace("Executing :sql", args.SQL(query))

	_, err = db.Exec(query)
	if err != nil {
		xray.ROOT.Fork().Alert("Error adding comment to :name - :err", args.Name(table), args.Error{Err: err})
		return err
	}

	return nil
}

// ShowCreateTable Execute `databaseOW CREATE TABLE schema.table`
func ShowCreateTable(database, table string) error {
	db, err := connect()
	if err != nil {
		return err
	}

	var query = "show create table " + database + "." + table
	xray.ROOT.Fork().Trace("Executing :sql", args.SQL(query))

	desc, err := db.Query(query)
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

func sqlTableStatus(database string, comment, part bool) string {
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
	return query
}

// ShowTables Show tables for db schema
func ShowTables(database string, comment, part bool) ([]Table, error) {
	db, err := connect()
	if err != nil {
		return nil, err
	}

	var query = sqlTableStatus(database, comment, part)
	xray.ROOT.Fork().Trace("Executing :sql", args.SQL(query))

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
	db, err := connect()
	if err != nil {
		return false, err
	}

	var query = "show tables in " + database + " like '" + table + "'"
	xray.ROOT.Fork().Trace("Executing :sql", args.SQL(query))

	var res sql.NullString
	if err := db.QueryRow(query).Scan(&res); err != nil {
		return false, err
	}

	return res.Valid, nil
}

// Partition Structure to represent partition
type Partition struct {
	Name       string
	Expression string
	Limiter    int
}

// GetPartitions database rows info about partitions, bool flag identifies table doesn't partitioned or does not exist at all
func GetPartitions(database, table string) ([]Partition, bool, string, error) {
	db, err := connect()
	if err != nil {
		return nil, false, "", err
	}

	var query = "show create table " + database + "." + table
	xray.ROOT.Fork().Trace("Executing :sql", args.SQL(query))

	rows, err := db.Query(query)

	if err != nil {
		return nil, false, "", err
	}

	var count int
	var description string
	for rows.Next() {
		var (
			name string
			dsc  string
		)
		err = rows.Scan(&name, &dsc)
		if err := rows.Scan(&name, &dsc); err != nil {
			return nil, false, "", err
		}
		description = dsc
		count++
	}

	// Table does not exist
	if description == "" {
		return []Partition{}, false, "", nil
	}

	partitioned := strings.Contains(description, "PARTITION BY RANGE")

	// Table exist but not partitioned
	if !partitioned {
		return []Partition{}, true, "", nil
	}

	var comment, column string
	var partitions []Partition

	matched := showTablePattern.FindAllStringSubmatch(description, -1)

	for _, match := range matched {
		if match[1] != "" { // COMMENT='(\[GM:.*])'
			comment = match[1]
		} else if match[2] != "" { // PARTITION BY RANGE \(.(\w+)
			column = match[2]
		} else if match[3] != "" && match[4] != "" { // PARTITION (\w+) VALUES LESS THAN \((\d+)
			limiter, err := strconv.Atoi(match[4])

			if err != nil {
				return nil, false, "", err
			}

			partitions = append(partitions, Partition{
				Name:       match[3],
				Expression: column,
				Limiter:    limiter,
			})
		}
	}

	return partitions, true, comment, nil
}

func sqlAddPartitions(database, table string, partitions map[string]int64) string {
	// Build sql for each partition
	var ps []string
	for n, l := range partitions {
		ps = append(ps, " partition "+n+" values less than ("+strconv.FormatInt(l, 10)+") ")
	}
	return fmt.Sprintf(
		`alter table %s.%s  add partition ( %s )`,
		database, table, strings.Join(ps[:], ","),
	)
}

// AddPartitions Add partitions to existing partitioned table
func AddPartitions(database, table string, partitions map[string]int64) error {
	if len(partitions) == 0 {
		//Nothing to alter
		return nil
	}
	db, err := connect()
	if err != nil {
		return err
	}

	// Alter
	var query = sqlAddPartitions(database, table, partitions)
	xray.ROOT.Fork().Trace("Executing :sql", args.SQL(query))

	_, err = db.Query(query)

	return err
}

// DropPartition Drop partition(s) by name
func DropPartition(database, table string, partitions []string) error {
	db, er := connect()
	if er != nil {
		return er
	}

	var query = fmt.Sprintf(
		`alter table %s.%s  drop partition %s`,
		database, table, strings.Join(partitions, ","),
	)
	xray.ROOT.Fork().Trace("Executing :sql", args.SQL(query))

	_, err := db.Query(query)

	return err
}

// CreateTableDuplicate Create duplicate from table without partitions
func CreateTableDuplicate(database, table, duplicateTable string) error {
	db, err := connect()
	if err != nil {
		return err
	}

	logger := xray.ROOT.Fork()

	// Create duplicate table
	var query = fmt.Sprintf(
		`create table %s.%s LIKE %s.%s`,
		database, duplicateTable, database, table,
	)
	logger.Trace("Executing :sql", args.SQL(query))

	_, err = db.Exec(query)
	if err != nil {
		return err
	}

	// Remove partitions
	query = fmt.Sprintf(
		`alter table %s.%s remove partitioning`,
		database, duplicateTable,
	)
	logger.Trace("Executing :sql", args.SQL(query))

	_, err = db.Exec(query)
	if err != nil {
		return err
	}

	// Change comment
	query = fmt.Sprintf(
		`alter table %s.%s comment 'Backup for %s'`,
		database, duplicateTable, table,
	)
	logger.Trace("Executing :sql", args.SQL(query))

	_, err = db.Exec(query)

	return err
}

// ExchangePartition Copy partition data to another table
func ExchangePartition(database, table, duplicateTable, name string) error {
	db, err := connect()
	if err != nil {
		return err
	}

	// Copy partition
	var query = fmt.Sprintf(
		`alter table %s.%s exchange partition %s with table %s.%s`,
		database, table, name, database, duplicateTable,
	)
	xray.ROOT.Fork().Trace("Executing :sql", args.SQL(query))

	_, err = db.Exec(query)

	return err
}

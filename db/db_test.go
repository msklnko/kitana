package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSqlAddPartitions(t *testing.T) {
	query := sqlAddPartitions("database", "table", map[string]int64{"first": 1, "second": 2})
	assert.Equalf(
		t,
		query,
		"alter table database.table  add partition (partition first values less than (1),partition second values less than (2))",
		"Two queries should be the same.",
	)
}

func TestSqlPartitionTable(t *testing.T) {
	query := sqlPartitionTable(
		"database",
		"table",
		"createdAt",
		map[string]time.Time{
			"first":  time.Unix(1, 0),
			"second": time.Unix(2, 0),
		},
	)
	assert.Equalf(
		t,
		query,
		"alter table database.table partition by range (createdAt) (partition first values less than (1),partition second values less than (2))",
		"Two queries should be the same.",
	)
}

func TestSqlALterPartitions(t *testing.T) {
	query := sqlALterPartitions("database", "table", []string{"id", "createdAt"})

	assert.Equalf(
		t,
		query,
		"alter table database.table drop primary key, add primary key (id, createdAt)",
		"Two queries should be the same.",
	)
}

func TestSqlTableStatus(t *testing.T) {
	query := sqlTableStatus("database", false, false)
	assert.Equalf(
		t,
		query,
		"show table status from database",
		"Two queries should be the same.",
	)
}

func TestSqlTableStatus_Partitioned(t *testing.T) {
	query := sqlTableStatus("database", true, true)
	assert.Equalf(
		t,
		query,
		"show table status from database where `comment` like '%GM%'",
		"Two queries should be the same.",
	)
}

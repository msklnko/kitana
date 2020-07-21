package db

import (
	"testing"

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

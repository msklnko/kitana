package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSqlAddPartitions(t *testing.T) {
	query := sqlAddPartitions("database", "table", map[string]int64{"first": 1, "second": 2})
	assert.Equalf(t, query, "", "Two queries should be the same.")
}

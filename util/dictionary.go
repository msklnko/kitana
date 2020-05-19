package util

import "errors"

// RetentionPolicy - retention policy for partitioned tables
type RetentionPolicy string

const (
	n = "none"
	d = "drop"
	b = "backup"
)

func isValid(rp RetentionPolicy) {
	switch rp {
	case n, b, d:
	}
	Er(errors.New("invalid retention policy"))
}

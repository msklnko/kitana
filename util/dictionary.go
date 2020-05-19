package util

import "errors"

type RetentionPolicy string

const (
	N = "none"
	D = "drop"
	B = "backup"
)

func isValid(rp RetentionPolicy) {
	switch rp {
	case N, D, B:
	}
	Er(errors.New("invalid retention policy"))
}

package cmt

import (
	"errors"
	"github.com/msklnko/kitana/util"
)

// RetentionPolicy - retention policy for partitioned tables
type RetentionPolicy string

const (
	n = "none"
	d = "drop"
	b = "backup"
)

func (p RetentionPolicy) toString() string {
	return string(p)
}

func toRP(rp string) RetentionPolicy {
	switch rp {
	case "n":
		return n
	case "d":
		return d
	case "b":
		return b
	}
	util.Er(errors.New("invalid retention policy"))
	return "unknown" //TODO
}

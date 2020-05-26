package cmt

import (
	"errors"
	"github.com/msklnko/kitana/util"
)

// Type of partitioning (daily/monthly)
type Type string

const (
	ml = "monthly"
	dl = "daily"
)

func (t Type) toString() string {
	return string(t)
}

func toType(tp string) Type {
	switch tp {
	case "ml":
		return ml
	case "d":
		return dl
	}
	util.Er(errors.New("invalid partitioning type"))
	return "unknown" //TODO ask
}

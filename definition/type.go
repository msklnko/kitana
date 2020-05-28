package definition

import (
	"errors"
)

// Type of partitioning (daily/monthly)
type Type string

var allTypes = []Type{Ml, Dl}

const (
	Ml = "ml"
	Dl = "dl"
)

func (t Type) String() string {
	return string(t)
}

func ToType(tp string) (*Type, error) {
	for _, t := range allTypes {
		if t.String() == tp {
			return &t, nil
		}
	}
	return nil, errors.New("invalid partitioned type")
}

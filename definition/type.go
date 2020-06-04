package definition

import (
	"errors"
)

// Type of partitioning (daily/monthly)
type Type string

var allTypes = []Type{Ml, Dl}

// Ml = monthly,  Dl = daily
const (
	Ml = "ml"
	Dl = "dl"
)

func (t Type) String() string {
	return string(t)
}

// ToType Convert type from string to Type
func ToType(tp string) (*Type, error) {
	for _, t := range allTypes {
		if t.String() == tp {
			return &t, nil
		}
	}
	return nil, errors.New("invalid partitioned type")
}

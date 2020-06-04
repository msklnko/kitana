package definition

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Parse Parse comment from table to definition structure
func Parse(cmt string) (*Definition, error) {
	definition := Definition{}
	if cmt == "" {
		return &definition, errors.New("comment is empty")
	}

	if !CommentPattern.MatchString(cmt) {
		return &definition, fmt.Errorf("comment %s did not match with partitioning rules", cmt)
	}

	parts := strings.Split(cmt[1:len(cmt)-1], ":")
	if len(parts) != 5 {
		return nil, fmt.Errorf("comment %s did not match with partitioning rules", cmt)
	}

	cnt, err := strconv.Atoi(parts[4])
	if err != nil {
		return nil, err
	}

	rp, err := ToRP(parts[3])
	if err != nil {
		return nil, fmt.Errorf("comment %s has invalid retention policy", cmt)
	}

	tp, err := ToType(parts[2])
	if err != nil {
		return nil, fmt.Errorf("comment %s has invalid partitioned type", cmt)
	}

	definition = Definition{
		Column:        parts[1],
		PartitionType: *tp,
		Rp:            *rp,
		Count:         cnt,
	}

	return &definition, nil
}

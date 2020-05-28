package definition

import (
	"errors"
	"strconv"
	"strings"

	"github.com/msklnko/kitana/util"
)

// Parse Parse comment from table to definition structure
func Parse(cmt string) (*Definition, error) {
	definition := Definition{}
	if cmt == "" {
		return &definition, errors.New("")
	}

	if !CommentPattern.MatchString(cmt) {
		return &definition, errors.New("comment " + cmt + " did not match with partitioning rules")
	}

	parts := strings.Split(cmt[1:len(cmt)-1], ":")
	if len(parts) != 5 {
		return nil, errors.New("comment " + cmt + " did not match with partitioning rules")
	}

	cnt, err := strconv.Atoi(parts[4])
	util.Er(err)

	rp, err := ToRP(parts[3])
	if err != nil {
		return nil, errors.New("comment " + cmt + " has invalid retention policy")
	}

	tp, err := ToType(parts[2])
	if err != nil {
		return nil, errors.New("comment " + cmt + " has invalid partitioned type")
	}

	definition = Definition{
		Column:        parts[1],
		PartitionType: *tp,
		Rp:            *rp,
		Count:         cnt,
	}

	return &definition, nil
}

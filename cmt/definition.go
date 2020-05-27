package cmt

import (
	"github.com/msklnko/kitana/util"
	"regexp"
	"strconv"
	"strings"
)

// CmtPattern Partitioned comment pattern
var CommentPattern *regexp.Regexp = regexp.MustCompile(`(?m)^\[GM:\w+:(ml|dl):(d|n|b):\d\]$`)
var PartIdentification string = "GM"

func Def(cmt string) (*Definition, string) {
	definition := Definition{}
	if cmt == "" {
		return &definition, ""
	}

	if !CommentPattern.MatchString(cmt) {
		return &definition, "comment " + cmt + " did not match with partitioning rules"
	}

	parts := strings.Split(cmt[1:len(cmt)-1], ":")
	if len(parts) != 5 {
		return &definition, "comment " + cmt + " did not match with partitioning rules"
	}

	cnt, err := strconv.Atoi(parts[4])
	util.Er(err)
	definition = Definition{
		Column:        parts[1],
		PartitionType: ToType(parts[2]),
		Rp:            toRP(parts[3]),
		Count:         cnt,
	}

	return &definition, "Partitioned by:`" + definition.Column +
		"`; type:`" + definition.PartitionType.toString() +
		"`; retention policy:`" + definition.Rp.toString() +
		"`; count:" + strconv.Itoa(definition.Count)
}

// Comment structure
type Definition struct {
	Column        string          // column name for partitioning
	PartitionType Type            // partitioning type
	Rp            RetentionPolicy // retention policy
	Count         int             // retention policy old partitions count
}

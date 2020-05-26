package cmt

import (
	"github.com/msklnko/kitana/util"
	"strconv"
	"strings"
)

func Def(cmt string) string {
	if cmt == "" {
		return ""
	}

	if !util.CmtPattern.MatchString(cmt) {
		return "comment " + cmt + " did not match with partitioning rules"
	}

	parts := strings.Split(cmt[1:len(cmt)-1], ":")
	if len(parts) != 5 {
		return "comment " + cmt + " did not match with partitioning rules"
	}

	cnt, err := strconv.Atoi(parts[4])
	util.Er(err)
	definition := Definition{
		column:        parts[1],
		partitionType: parts[2],
		rp:            toRP(parts[3]),
		count:         cnt,
	}

	return "Partitioned by:`" + definition.column +
		"`; type:`" + definition.partitionType +
		"`; retention policy:`" + definition.rp.toString() +
		"`; count:" + strconv.Itoa(definition.count)
}

// Comment structure
type Definition struct {
	column        string          //column name for partitioning
	partitionType string          // partitioning type
	rp            RetentionPolicy // retention policy
	count         int             // retention policy old partitions count
}

package definition

import (
	"regexp"
	"strconv"
)

// CmtPattern Partitioned comment pattern
var CommentPattern = regexp.MustCompile(`(?m)^\[GM:\w+:(ml|dl):([dnb]):\d]$`)
var PartIdentification = "GM"

// Comment structure
type Definition struct {
	Column        string          // column name for partitioning
	PartitionType Type            // partitioning type
	Rp            RetentionPolicy // retention policy
	Count         int             // retention policy old partitions count
}

func (definition Definition) String() string {
	return "Partitioned by:`" + definition.Column +
		"`; type:`" + definition.PartitionType.String() +
		"`; retention policy:`" + definition.Rp.toString() +
		"`; count:" + strconv.Itoa(definition.Count)
}

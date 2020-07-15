package partition

import (
	"regexp"

	"github.com/spf13/cobra"
)

var prtCount = regexp.MustCompile(`(?m)^\+\d*$`)

// PartitionCmd is a main command for partitioning
var PartitionCmd = &cobra.Command{
	Use:     "partition",
	Aliases: []string{"prt"},
	Short:   "Used either to obtain information about information_schema.partitions",
}

func init() {
	PartitionCmd.PersistentFlags().BoolP("show", "s", false, "Show partitions")

	PartitionCmd.AddCommand(
		statusCmd,
		addCmd,
		dropCmd,
	)
}

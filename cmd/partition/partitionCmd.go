package partition

import (
	"regexp"

	"github.com/spf13/cobra"
)

var prtCount = regexp.MustCompile(`(?m)^\+\d*$`)
var show bool

// PartitionCmd is a main command for partitioning
var PartitionCmd = &cobra.Command{
	Use:     "partition",
	Aliases: []string{"prt"},
	Short:   "Used either to obtain information about partitions",
}

func init() {
	PartitionCmd.PersistentFlags().BoolVarP(&show, "show", "s", false, "Show partitions")

	PartitionCmd.AddCommand(
		statusCmd,
		addCmd,
		dropCmd,
	)
}

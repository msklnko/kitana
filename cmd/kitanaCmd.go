package cmd

import (
	"github.com/msklnko/kitana/cmd/partition"
	"github.com/spf13/cobra"
)

// KitanaCmd main command
var KitanaCmd = &cobra.Command{}

func init() {
	KitanaCmd.AddCommand(
		partition.PartitionCmd,
		showCmd,
		commentCmd,
		daemonCmd,
		testCmd,
		indexCmd,
	)
}

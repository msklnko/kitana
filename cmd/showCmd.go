package cmd

import (
	"github.com/mono83/xray"
	"github.com/msklnko/kitana/partition"
	"github.com/spf13/cobra"
)

// Show all tables from db with partition configs
var showCmd = &cobra.Command{
	Use:   "show",
	Short: "Show all tables",
	RunE: func(cmd *cobra.Command, args []string) error {
		partitioned, err := cmd.Flags().GetBool("partitioned")
		comment, err := cmd.Flags().GetBool("comment")
		def, err := cmd.Flags().GetBool("definition")
		if err != nil {
			return err
		}

		xray.BOOT.Info("Incoming request `show tables`")

		var database = ""
		if len(args) > 0 {
			database = args[0]
		}
		return partition.ShowTables(database, comment, partitioned, def, xray.ROOT.Fork())
	},
}

func init() {
	showCmd.Flags().BoolP("partitioned", "p", false, "Show only partitioned tables")
	showCmd.Flags().BoolP("comment", "c", false, "Show only commented tables")
	showCmd.Flags().BoolP("definition", "d", false, "Show comment definition")
}

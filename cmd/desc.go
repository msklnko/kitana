package cmd

import (
	"fmt"
	"os"

	"github.com/mono83/xray"
	"github.com/msklnko/kitana/partition"
	"github.com/spf13/cobra"
)

// Show all tables from db with partition configs
var showCmd = &cobra.Command{
	Use:   "show",
	Short: "Show all tables",
	Run: func(cmd *cobra.Command, args []string) {
		partitioned, err := cmd.Flags().GetBool("partitioned")
		comment, err := cmd.Flags().GetBool("comment")
		def, err := cmd.Flags().GetBool("definition")
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		logger := xray.ROOT.Fork()
		logger.Info("Incoming request `show tables`")

		var database = ""
		if len(args) > 0 {
			database = args[0]
		}
		err = partition.ShowTables(database, comment, partitioned, def, logger)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	},
}

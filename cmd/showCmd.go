package cmd

import (
	"github.com/mono83/xray"
	"github.com/msklnko/kitana/config"
	"github.com/msklnko/kitana/partition"
	"github.com/spf13/cobra"
)

var partitioned, commented, description bool

// Show all tables from db with partition configs
var showCmd = &cobra.Command{
	Use:   "show",
	Short: "Show all tables",
	RunE: func(cmd *cobra.Command, args []string) error {
		xray.BOOT.Info("Incoming request `show tables`")

		var database = ""
		if len(args) > 0 {
			database = args[0]
		}

		db, err := config.Connect()
		if err != nil {
			return err
		}

		return partition.ShowTables(db, database, commented, partitioned, description, xray.ROOT.Fork())
	},
}

func init() {
	showCmd.Flags().BoolVarP(&partitioned, "partitioned", "p", false, "Show only partitioned tables")
	showCmd.Flags().BoolVarP(&commented, "comment", "c", false, "Show only commented tables")
	showCmd.Flags().BoolVarP(&description, "definition", "d", false, "Show comment definition")
}

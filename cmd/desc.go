package cmd

import (
	"errors"
	"github.com/msklnko/kitana/db"
	"github.com/msklnko/kitana/util"
	"github.com/spf13/cobra"
)

// Show all tables from db with partition configs
var showCmd = &cobra.Command{
	Use:   "show",
	Short: "Show all tables",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("missed schema name")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		partitioned, err := cmd.Flags().GetBool("partitioned")
		comment, err := cmd.Flags().GetBool("comment")
		def, err := cmd.Flags().GetBool("definition")
		util.Er(err)
		db.ShowTables(args[0], comment, partitioned, def)
	},
}

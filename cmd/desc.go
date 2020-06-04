package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/msklnko/kitana/db"
	"github.com/spf13/cobra"
)

// Show all tables from db with partition configs
var showCmd = &cobra.Command{
	Use:   "show",
	Short: "Show all tables",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("missed database")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		partitioned, err := cmd.Flags().GetBool("partitioned")
		comment, err := cmd.Flags().GetBool("comment")
		def, err := cmd.Flags().GetBool("definition")
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		db.ShowTables(args[0], comment, partitioned, def)
	},
}

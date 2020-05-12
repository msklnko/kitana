package cmd

import (
	"errors"
	"github.com/msklnko/kitana/db"
	"github.com/spf13/cobra"
	"strings"
)

// Show table structure
var partCmd = &cobra.Command{
	Use:   "prt",
	Short: "Used either to obtain information about information_schema.partitions",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("table name is missing")
		}
		var tbls = strings.Split(args[0], ".")
		if len(tbls) != 2 {
			return errors.New("invalid property, should be schema+table name")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		var tbls = strings.Split(args[0], ".")
		db.InformSchema(tbls[0], tbls[1])
	},
}

package cmd

import (
	"errors"
	"github.com/msklnko/kitana/db"
	"github.com/msklnko/kitana/util"
	"github.com/spf13/cobra"
	"strings"
)

var prtCmd = &cobra.Command{
	Use:   "prt",
	Short: "Used either to obtain information about information_schema.partitions",
}

var prtAdd = &cobra.Command{
	Use:   "add",
	Short: "Add partition",
	Args: func(cmd *cobra.Command, args []string) error {
		switch l := len(args); l {
		case 0:
			return errors.New("missing arguments (table, name, limiter)")
		case 1:
			var tbls = strings.Split(args[0], ".")
			if len(tbls) != 2 {
				return errors.New("invalid property, should be schema+table name")
			}
			if !db.CheckTablePresent(tbls[0], tbls[1]) {
				return errors.New("Table " + args[0] + " does not exist")
			}
			return errors.New("partition name and limiter are missing")
		case 2:
			return errors.New("limiter is missing")
		default:
			return nil
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		var tbls = strings.Split(args[0], ".")
		db.AddPartition(tbls[0], tbls[1], args[1], args[2])

		show, err := cmd.Flags().GetBool("show")
		util.Er(err)

		if show {
			db.InformSchema(tbls[0], tbls[1])
		}
	},
}

var prtStatus = &cobra.Command{
	Use:     "status",
	Aliases: []string{"st", "info"},
	Short:   "Show info about partitions",
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

var prtDrop = &cobra.Command{
	Use:     "drop",
	Aliases: []string{"rm"},
	Short:   "Drop partition",
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
		db.DropPartition(tbls[0], tbls[1], args[1])

		show, err := cmd.Flags().GetBool("show")
		util.Er(err)

		if show {
			db.InformSchema(tbls[0], tbls[1])
		}
	},
}

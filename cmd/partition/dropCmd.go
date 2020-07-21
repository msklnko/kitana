package partition

import (
	"errors"
	"strings"

	"github.com/msklnko/kitana/config"
	"github.com/msklnko/kitana/db"
	"github.com/msklnko/kitana/partition"
	"github.com/spf13/cobra"
)

var dropCmd = &cobra.Command{
	Use:     "drop",
	Aliases: []string{"rm"},
	Short:   "Drop partition",
	Args: func(cmd *cobra.Command, args []string) error {
		switch l := len(args); l {
		case 0:
			return errors.New("missing arguments (table, partition name)")
		case 1:
			var tables = strings.Split(args[0], ".")
			if len(tables) != 2 {
				return errors.New("invalid property, should be schema+table name")
			}
			connection, err := config.Connect()
			if err != nil {
				return err
			}
			present, err := db.CheckTablePresent(connection, tables[0], tables[1])
			if err != nil {
				return err
			}
			if !present {
				return errors.New("table " + args[0] + " does not exist")
			}
			return errors.New("partition name is missing")
		default:
			return nil
		}
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		var tables = strings.Split(args[0], ".")
		connection, err := config.Connect()
		if err != nil {
			return err
		}
		err = db.DropPartition(connection, tables[0], tables[1], []string{args[1]})
		if err != nil {
			return err
		}

		if show {
			err := partition.PartitionsInfo(connection, tables[0], tables[1])
			if err != nil {
				return err
			}
		}
		return nil
	},
}

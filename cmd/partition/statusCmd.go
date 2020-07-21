package partition

import (
	"errors"
	"strings"

	"github.com/msklnko/kitana/config"
	"github.com/msklnko/kitana/partition"
	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:     "status",
	Aliases: []string{"st", "info"},
	Short:   "Show info about partitions (example: `kitana partition status database.table`)",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("table name is missing")
		}
		var tables = strings.Split(args[0], ".")
		if len(tables) != 2 {
			return errors.New("invalid property, should be schema+table name")
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		var tables = strings.Split(args[0], ".")

		connection, err := config.Connect()
		if err != nil {
			return err
		}

		err = partition.PartitionsInfo(connection, tables[0], tables[1])
		if err != nil {
			return err
		}
		return nil
	},
}

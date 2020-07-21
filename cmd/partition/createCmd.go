package partition

import (
	"errors"
	"github.com/msklnko/kitana/config"
	"github.com/msklnko/kitana/partition"
	"strings"

	"github.com/spf13/cobra"
)

var createCountNewPartitions int

var createCmd = &cobra.Command{
	Use:     "create",
	Aliases: []string{""},
	Short:   "Actualize partitions for defined table",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("table name is required")
		}
		var tables = strings.Split(args[0], ".")
		if len(tables) != 2 {
			return errors.New("invalid property, should be schema.table name")
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		var tables = strings.Split(args[0], ".")

		connection, err := config.Connect()
		if err != nil {
			return err
		}

		err = partition.PartitionTable(connection, tables[0], tables[1], createCountNewPartitions)
		return err
	},
}

func init() {
	PartitionCmd.PersistentFlags().IntVarP(
		&createCountNewPartitions,
		"count",
		"c",
		3,
		"Number of partitions to create in advance, default = 3",
	)
}

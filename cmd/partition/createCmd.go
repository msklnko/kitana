package partition

import (
	"errors"
	"strings"

	"github.com/mono83/xray"
	"github.com/msklnko/kitana/partition"
	"github.com/spf13/cobra"
)

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
		splitted := strings.Split(args[0], ".")
		if err := partition.ManagePartitions(splitted[0], splitted[1], forceDelete, xray.ROOT.Fork()); err != nil {
			return err
		}
		return nil
	},
}

func init() {
	createCmd.Flags().BoolVarP(
		&forceDelete,
		"forceDelete",
		"f",
		false,
		"Delete partitions with one alter",
	)
}

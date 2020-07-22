package partition

import (
	"errors"
	"github.com/msklnko/kitana/config"
	"strings"
	"time"

	"github.com/mono83/xray"
	"github.com/msklnko/kitana/partition"
	"github.com/spf13/cobra"
)

var actualizeForceDelete bool
var actualizeDropInterval time.Duration

var actualizeCmd = &cobra.Command{
	Use:     "actualize",
	Aliases: []string{"update", "manage"},
	Short:   "Actualize partitions for defined table (example: `kitana partition actualize database.table`)",
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

		connection, err := config.Connect()
		if err != nil {
			return err
		}

		if err := partition.ManagePartitions(
			connection,
			splitted[0],
			splitted[1],
			actualizeForceDelete,
			actualizeDropInterval,
			xray.ROOT.Fork(),
		); err != nil {
			return err
		}
		return nil
	},
}

func init() {
	actualizeCmd.Flags().BoolVarP(
		&actualizeForceDelete,
		"forceDelete",
		"f",
		false,
		"Delete partitions with one alter",
	)

	actualizeCmd.Flags().DurationVarP(
		&actualizeDropInterval,
		"dropInterval",
		"d",
		500*time.Millisecond,
		"Daemon drop partitions interval",
	)
}

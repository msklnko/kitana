package cmd

import (
	"errors"
	"time"

	"github.com/msklnko/kitana/partition"
	"github.com/spf13/cobra"
)

var demon = &cobra.Command{
	Use:   "daemon",
	Short: "Run partitioning in daemon",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("schema name is missing")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {

		var interval = time.Second * 30 // default recurrence time
		partition.ManageAllDatabasePartitions(args[0], interval)
	},
}

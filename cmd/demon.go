package cmd

import (
	"context"
	"time"

	"github.com/msklnko/kitana/partition"
	"github.com/msklnko/kitana/scheduler"
	"github.com/spf13/cobra"
)

var demon = &cobra.Command{
	Use:   "daemon",
	Short: "Run partitioning in daemon",
	Args: func(cmd *cobra.Command, args []string) error {
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {

		var interval = time.Second * 30 // default recurrence time
		parentContext := context.Background()
		newScheduler := scheduler.NewScheduler()
		newScheduler.Register(parentContext, partition.ManageAllDatabasePartitions, interval)
	},
}

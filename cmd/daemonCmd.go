package cmd

import (
	"errors"
	"time"

	"github.com/msklnko/kitana/config"
	"github.com/msklnko/kitana/partition"
	"github.com/spf13/cobra"
)

var daemonRefreshInterval time.Duration
var forceDelete bool

var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "Run partitioning in daemon (example: `kitana daemon database`)",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("schema name is missing")
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		if daemonRefreshInterval < 30*time.Second {
			return errors.New("illegal refresh interval " + daemonRefreshInterval.String())
		}
		db, err := config.Connect()
		if err != nil {
			return err
		}

		partition.ManageAllDatabasePartitions(db, args[0], forceDelete, daemonRefreshInterval)
		return nil
	},
}

func init() {
	daemonCmd.Flags().DurationVarP(
		&daemonRefreshInterval,
		"refresh",
		"r",
		time.Second*30,
		"Daemon refresh interval",
	)

	daemonCmd.Flags().BoolVarP(
		&forceDelete,
		"forceDelete",
		"f",
		false,
		"Delete partitions with one alter",
	)
}

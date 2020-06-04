package partition

import (
	"context"
	"time"

	"github.com/mono83/xray"
	"github.com/mono83/xray/args"
	"github.com/msklnko/kitana/db"
)

// ManageAllDatabasePartitions checking all tables partitioning
func ManageAllDatabasePartitions(ctx context.Context) {
	logger := xray.ROOT.Fork()
	logger.Info("Executing manage partition task :time", args.String{N: "time", V: time.Now().UTC().String()})

	tables, err := db.ShowTables("wallet_wtc", true, true)
	if err != nil {
		logger.Error("Unable to get partitioned tables :err", args.Error{Err: err})
		return
	}

	for _, table := range tables {
		logger.Debug("Checking :name table", args.Name(table.Name))

		// Manage
		err := ManagePartitions("wallet_wtc", table.Name, logger)

		if err != nil {
			logger.Error("Error occurs while managing partitions :err", args.Error{Err: err})
			continue
		}
	}
}

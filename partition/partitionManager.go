package partition

import (
	"time"

	"github.com/mono83/xray"
	"github.com/mono83/xray/args"
	"github.com/msklnko/kitana/db"
)

// ManageAllDatabasePartitions checking all tables partitioning
func ManageAllDatabasePartitions(database string, interval time.Duration) {
	time.Sleep(interval)

	logger := xray.ROOT.Fork()
	logger.Info("Executing manage partition task :time", args.String{N: "time", V: time.Now().UTC().String()})

	tables, err := db.ShowTables(database, true, true)
	if err != nil {
		logger.Error("Unable to get partitioned tables :err", args.Error{Err: err})
		return
	}

	for _, table := range tables {
		logger.Debug("Checking :name table", args.Name(table.Name))

		// Manage
		err := ManagePartitions(table.Database, table.Name, logger)

		if err != nil {
			logger.Error("Error occurs while managing partitions :err", args.Error{Err: err})
			continue
		}
	}
	ManageAllDatabasePartitions(database, interval)
}

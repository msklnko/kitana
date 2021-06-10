package partition

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/mono83/xray"
	"github.com/mono83/xray/args"
	"github.com/msklnko/kitana/db"
)

// ManageAllDatabasePartitions checking all tables partitioning
func ManageAllDatabasePartitions(
	connection *sql.DB,
	database []string,
	forceDelete bool,
	refreshInterval time.Duration,
	dropInterval time.Duration,
) {
	logger := xray.ROOT.Fork()
	logger.Info("Waiting :time seconds", args.String{
		N: "time",
		V: fmt.Sprintf("%.0f", refreshInterval.Seconds()),
	})
	time.Sleep(refreshInterval)

	logger.Info("Executing manage partition task :time", args.String{N: "time", V: time.Now().UTC().String()})

	tables, err := db.ShowTables(connection, database, true, true)
	if err != nil {
		logger.Error("Unable to get partitioned tables :err", args.Error{Err: err})
		return
	}

	for _, table := range tables {
		logger.Debug("Checking :name table", args.Name(table.Name))

		// Manage
		err := ManagePartitions(connection, table.Database, table.Name, forceDelete, dropInterval, logger)

		if err != nil {
			logger.Error("Error occurs while managing partitions :err", args.Error{Err: err})
			continue
		}
	}
	ManageAllDatabasePartitions(connection, database, forceDelete, refreshInterval, dropInterval)
}

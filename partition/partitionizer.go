package partition

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mono83/xray"
	"github.com/mono83/xray/args"
	"github.com/msklnko/kitana/db"
	"github.com/msklnko/kitana/definition"
	"github.com/msklnko/kitana/util"
)

// ManagePartitions Add next partition if not exist
func ManagePartitions(connection *sql.DB, database, table string, forceDelete bool, logger xray.Ray) error {
	partitions, exist, comment, err := db.GetPartitions(connection, database, table)

	if err != nil {
		return err
	}

	if !exist {
		return errors.New("table " + database + "." + table + " doesn't exist\n")
	}

	if len(partitions) == 0 {
		return errors.New("table " + database + "." + table + " is not partitioned\n")
	}

	// Parse comment
	rule, err := definition.Parse(comment)
	if err != nil {
		return err
	}

	// Create next partitions
	err = ensureNextPartition(connection, database, table, rule, partitions, logger)
	if err != nil {
		return err
	}

	// Need to delete unnecessary
	if rule.Rp == definition.D || rule.Rp == definition.B {
		err := removeOldPartitions(connection, database, table, rule, forceDelete, logger)
		if err != nil {
			return err
		}
	}

	return nil
}

func ensureNextPartition(
	connection *sql.DB,
	database, table string,
	rule *definition.Definition,
	partitions []db.Partition,
	logger xray.Ray,
) error {
	// Sort partitions (just in case)
	sort.SliceStable(partitions, func(i, j int) bool {
		return partitions[i].Limiter < partitions[j].Limiter
	})

	// Calculate next partition
	nextPartitionName, nextPartitionLimiter, err := NextOne(rule.PartitionType, logger)
	if err != nil {
		return err
	}

	// Collect partition names
	var existedPartitions []string
	for _, partition := range partitions {
		existedPartitions = append(existedPartitions, partition.Name)
	}

	// Check if partition creation needs
	if util.Contains(existedPartitions, *nextPartitionName) {
		logger.Info("Partition :name for :table is already exists",
			args.Name(*nextPartitionName), args.String{N: "table", V: database + "." + table})
		return nil
	}

	// Alter partition
	err = db.AddPartitions(
		connection,
		database,
		table,
		map[string]int64{*nextPartitionName: nextPartitionLimiter.Unix()},
	)

	if err != nil {
		logger.Error("Partition :name for :table was not created because of :error",
			args.Name(*nextPartitionName), args.String{N: "table", V: database + table}, args.Error{Err: err})
		return err
	}

	logger.Info("Partition :name for :table was created",
		args.Name(*nextPartitionName), args.String{N: "table", V: database + table})

	return nil
}

func removeOldPartitions(
	connection *sql.DB, database,
	table string,
	rule *definition.Definition,
	forceDelete bool,
	logger xray.Ray,
) error {
	// Existed partitions
	updatedPartitions, _, _, err := db.GetPartitions(connection, database, table)
	if err != nil {
		logger.Error("")
		return err
	}

	// Partitions should keep alive
	keepAlive, err := KeepAlive(rule.PartitionType, rule.Count, logger)
	if err != nil {
		return err
	}

	// Detecting partitions to remove
	var remove []string
	for _, partition := range updatedPartitions {
		if !util.Contains(keepAlive, partition.Name) {
			remove = append(remove, partition.Name)
		}
	}
	if len(remove) > 0 {
		logger.Info("Partitions :name from :table would be removed",
			args.Name(strings.Join(remove, ",")),
			args.String{N: "table", V: database + "." + table})

		if rule.Rp == definition.B {
			for _, name := range remove {
				duplicateTable := table + "_" + name
				logger.Info(fmt.Sprintf("Creating backup for %s", duplicateTable))
				err := db.CreateTableDuplicate(connection, database, table, duplicateTable)
				if err != nil {
					logger.Error("Error occurs during backup partition process :name, :err",
						args.Name(database+"."+table), args.Error{Err: err})
					return err
				}
				err = db.ExchangePartition(connection, database, table, duplicateTable, name)
				if err != nil {
					logger.Error("Error occurs during exchange partition process :name, :err",
						args.Name(database+"."+table), args.Error{Err: err})
					return err
				}
			}
		}

		if forceDelete {
			logger.Info("Forced drop partitions")
			err := db.DropPartition(connection, database, table, remove)
			if err != nil {
				return err
			}
		} else {
			logger.Info("Drop partitions one by one")
			for _, partitionToRemove := range remove {
				err := db.DropPartition(connection, database, table, []string{partitionToRemove})
				if err != nil {
					return err
				}
				logger.Info(fmt.Sprintf("%s was removed", partitionToRemove))
				time.Sleep(500 * time.Millisecond)
			}
			logger.Info("Cleaning partitions were finished")
		}
	}
	return nil
}

func PartitionTable(connection *sql.DB, database, table string, count int) error {
	logger := xray.ROOT.Fork()
	logger.Info("Execution partition :name", args.Name(table))

	_, partitioned, comment, err := db.GetPartitions(connection, database, table)

	if err != nil {
		return err
	}
	if partitioned {
		return errors.New(fmt.Sprintf("Table %s is already paritioned", table))
	}

	// Parse comment
	parsedComment, err := definition.Parse(comment)
	if err != nil {
		return err
	}

	partitions, err := NextSeveral(parsedComment.PartitionType, count, true, logger)
	if err != nil {
		return err
	}

	err = db.PartitionTable(connection, database, table, parsedComment.Column, partitions)
	if err != nil {
		if strings.Contains(err.Error(), "A PRIMARY KEY must include all columns in") {

			index, err := db.GetPrimaryIndex(connection, database, table)
			if err != nil {
				return err
			}

			return errors.New(
				fmt.Sprintf(
					"A PRIMARY KEY must include all columns in the table's partitioning function,"+
						" existing PRIMARY KEY(%s) should be updated to (%s,`%s`), use `kitana index`",
					index,
					index,
					parsedComment.Column,
				),
			)
		}

		return err
	}

	logger.Info("Table :name partitioning was finished", args.Name(table))
	return nil
}

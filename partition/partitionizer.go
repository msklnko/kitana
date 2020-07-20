package partition

import (
	"errors"
	"fmt"
	"sort"
	s "strings"
	"time"

	"github.com/mono83/xray"
	"github.com/mono83/xray/args"
	"github.com/msklnko/kitana/db"
	"github.com/msklnko/kitana/definition"
	"github.com/msklnko/kitana/util"
)

// ManagePartitions Add next partition if not exist
func ManagePartitions(database, table string, forceDelete bool, logger xray.Ray) error {
	partitions, exist, comment, err := db.GetPartitions(database, table)

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
	err = ensureNextPartition(database, table, rule, partitions, logger)
	if err != nil {
		return err
	}

	// Need to delete unnecessary
	if rule.Rp == definition.D || rule.Rp == definition.B {
		err := removeOldPartitions(database, table, rule, forceDelete, logger)
		if err != nil {
			return err
		}
	}

	return nil
}

func ensureNextPartition(database, table string,
	rule *definition.Definition,
	partitions []db.Partition,
	logger xray.Ray) error {
	// Sort partitions (just in case)
	sort.SliceStable(partitions, func(i, j int) bool {
		return partitions[i].Limiter < partitions[j].Limiter
	})

	// Calculate next partition
	nextPartitionName, nextPartitionLimiter, err := Next(rule.PartitionType, logger)
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
	err = db.AddPartitions(database, table, map[string]int64{*nextPartitionName: nextPartitionLimiter.Unix()})

	if err != nil {
		logger.Error("Partition :name for :table was not created because of :error",
			args.Name(*nextPartitionName), args.String{N: "table", V: database + table}, args.Error{Err: err})
		return err
	}

	logger.Info("Partition :name for :table was created",
		args.Name(*nextPartitionName), args.String{N: "table", V: database + table})

	return nil
}

func removeOldPartitions(database, table string, rule *definition.Definition, forceDelete bool, logger xray.Ray) error {
	// Existed partitions
	updatedPartitions, _, _, err := db.GetPartitions(database, table)
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
			args.Name(s.Join(remove, ",")),
			args.String{N: "table", V: database + "." + table})

		if rule.Rp == definition.B {
			for _, name := range remove {
				duplicateTable := table + "_" + name
				logger.Info(fmt.Sprintf("Creating backup for %s", duplicateTable))
				err := db.CreateTableDuplicate(database, table, duplicateTable)
				if err != nil {
					logger.Error("Error occurs during backup partition process :name, :err",
						args.Name(database+"."+table), args.Error{Err: err})
					return err
				}
				err = db.ExchangePartition(database, table, duplicateTable, name)
				if err != nil {
					logger.Error("Error occurs during exchange partition process :name, :err",
						args.Name(database+"."+table), args.Error{Err: err})
					return err
				}
			}
		}

		if forceDelete {
			logger.Info("Forced drop partitions")
			err := db.DropPartition(database, table, remove)
			if err != nil {
				return err
			}
		} else {
			logger.Info("Drop partitions one by one")
			for _, partitionToRemove := range remove {
				err := db.DropPartition(database, table, []string{partitionToRemove})
				if err != nil {
					return err
				}
				logger.Info(fmt.Sprintf("%s was removed", partitionToRemove))
				time.Sleep(1 * time.Second)
			}
			logger.Info("Cleaning partitions were finished")
		}
	}
	return nil
}

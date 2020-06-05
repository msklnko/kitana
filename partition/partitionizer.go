package partition

import (
	"errors"
	"sort"
	s "strings"

	"github.com/mono83/xray"
	"github.com/mono83/xray/args"
	"github.com/msklnko/kitana/db"
	"github.com/msklnko/kitana/definition"
	"github.com/msklnko/kitana/util"
)

//// BatchAdd Add provided count of partitions
//func BatchAdd(sh, tb string, count int) error {
//	partitions, exist, comment := db.InformSchema(sh, tb)
//
//	if !exist {
//		return errors.New("table " + sh + "." + tb + " doesn't exist\n")
//	}
//
//	if len(partitions) == 0 {
//		return errors.New("table " + sh + "." + tb + " is not partitioned\n")
//	}
//
//	if !definition.CommentPattern.MatchString(comment) {
//		return errors.New("in order to partition the table " + sh + "." + tb + " need to add a comment, " +
//			"use `kitana cmt add `db.table_name` [GM:C:T:R:Rc]`")
//	}
//
//	// Sort partitions (just in case)
//	sort.SliceStable(partitions, func(i, j int) bool {
//		return partitions[i].Limiter < partitions[j].Limiter
//	})
//
//	// Last partition, identifier to start
//	lastPartition := partitions[len(partitions)-1]
//	lastDateLimiter := time.Unix(lastPartition.Limiter, 0)
//	lastPartitionName := lastPartition.Name
//
//	if !s.HasPrefix(lastPartitionName, "part") {
//		return errors.New("partitions should have prefix `part`")
//	}
//	//lastName := s.TrimPrefix(lastPartitionName, "prefix")
//
//	// Candidates
//	candidates := make(map[string]int64)
//
//	// Comment
//	def, _ := definition.Parse(comment)
//	var month = 0
//	var days = 0
//	//TODO ask
//	if def.PartitionType == definition.Ml {
//		month = 1
//	} else if def.PartitionType == definition.Dl {
//		days = 1
//	}
//
//	for i := 0; i < count; i++ {
//		//TODO day
//		year, m, _ := lastDateLimiter.Date()
//		lastDateLimiter = lastDateLimiter.AddDate(0, month, days)
//
//		// Need to search function
//		mnth := strconv.Itoa(int(m))
//		if len(mnth) != 2 {
//			mnth = "0" + mnth
//		}
//
//		candidates["part"+strconv.Itoa(year)+mnth] = lastDateLimiter.Unix()
//	}
//
//	db.AddPartitions(sh, tb, candidates)
//
//	//TODO Retention policy
//	return nil
//}

// ManagePartitions Add next partition if not exist
func ManagePartitions(database, table string, logger xray.Ray) error {
	partitions, exist, comment, err := db.InformSchema(database, table)

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
		err := removeOldPartitions(database, table, rule, logger)
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
			args.Name(*nextPartitionName), args.String{N: "table", V: database + table})
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

func removeOldPartitions(database, table string, rule *definition.Definition, logger xray.Ray) error {
	// Existed partitions
	updatedPartitions, _, _, err := db.InformSchema(database, table)
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

		err := db.DropPartition(database, table, remove)
		if err != nil {
			return err
		}
	}
	return nil
}

package partition

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	s "strings"
	"text/tabwriter"
	"time"

	"github.com/jinzhu/now"

	"github.com/mono83/xray"
	"github.com/mono83/xray/args"
	"github.com/msklnko/kitana/db"
	"github.com/msklnko/kitana/definition"
	"github.com/msklnko/kitana/util"
)

// BatchAdd Add provided count of partitions
func BatchAdd(sh, tb string, count int) error {
	partitions, exist, comment := db.InformSchema(sh, tb)

	if !exist {
		return errors.New("table " + sh + "." + tb + " doesn't exist\n")
	}

	if len(partitions) == 0 {
		return errors.New("table " + sh + "." + tb + " is not partitioned\n")
	}

	if !definition.CommentPattern.MatchString(comment) {
		return errors.New("in order to partition the table " + sh + "." + tb + " need to add a comment, " +
			"use `kitana cmt add `db.table_name` [GM:C:T:R:Rc]`")
	}

	// Sort partitions (just in case)
	sort.SliceStable(partitions, func(i, j int) bool {
		return partitions[i].Desc < partitions[j].Desc
	})

	// Last partition, identifier to start
	lastPartition := partitions[len(partitions)-1]
	lastDateLimiter := time.Unix(lastPartition.Desc, 0)
	lastPartitionName := lastPartition.Name

	if !s.HasPrefix(lastPartitionName, "part") {
		return errors.New("partitions should have prefix `part`")
	}
	//lastName := s.TrimPrefix(lastPartitionName, "prefix")

	// Candidates
	candidates := make(map[string]int64)

	// Comment
	def, _ := definition.Parse(comment)
	var month = 0
	var days = 0
	//TODO ask
	if def.PartitionType == definition.Ml {
		month = 1
	} else if def.PartitionType == definition.Dl {
		days = 1
	}

	for i := 0; i < count; i++ {
		//TODO day
		year, m, _ := lastDateLimiter.Date()
		lastDateLimiter = lastDateLimiter.AddDate(0, month, days)

		// Need to search function
		mnth := strconv.Itoa(int(m))
		if len(mnth) != 2 {
			mnth = "0" + mnth
		}

		candidates["part"+strconv.Itoa(year)+mnth] = lastDateLimiter.Unix()
	}

	db.AddPartitions(sh, tb, candidates)

	//TODO Retention policy
	return nil
}

// AddNextPartition Add next partition if not exist
func AddNextPartition(database, table string, logger xray.Ray) error {
	partitions, exist, comment := db.InformSchema(database, table)

	if !exist {
		return errors.New("table " + database + "." + table + " doesn't exist\n")
	}

	if len(partitions) == 0 {
		return errors.New("table " + database + "." + table + " is not partitioned\n")
	}

	// Sort partitions (just in case)
	sort.SliceStable(partitions, func(i, j int) bool {
		return partitions[i].Desc < partitions[j].Desc
	})

	// Collect partition names
	var existedPartitions []string
	for _, partition := range partitions {
		existedPartitions = append(existedPartitions, partition.Name)
	}

	// Parse comment
	rule, err := definition.Parse(comment)
	if err != nil {
		return err
	}

	// Calculate next partition
	var nextPartitionName = "part"
	var nextPartitionLimiter time.Time
	if rule.PartitionType == definition.Ml {
		date := time.Now().UTC().AddDate(0, 1, 0)
		nextPartitionName = nextPartitionName + date.Format("200601")
		nextPartitionLimiter = now.New(date.AddDate(0, 1, 0)).BeginningOfMonth()
	} else if rule.PartitionType == definition.Dl {
		date := time.Now().UTC().AddDate(0, 0, 1)
		nextPartitionName = nextPartitionName + date.Format("20060102")
		nextPartitionLimiter = now.New(date.AddDate(0, 0, 1)).BeginningOfDay()
	} else {
		return errors.New("not supported partition type " + rule.PartitionType.String())
	}

	// Check if partition creation needs
	if util.Contains(existedPartitions, nextPartitionName) {
		logger.Info("Partition :name for :table is already exists",
			args.Name(nextPartitionName), args.String{N: "table", V: database + table})
		return nil
	}

	// Alter partition
	err = db.AddPartition(database, table, nextPartitionName, nextPartitionLimiter.Unix())
	if err == nil {
		logger.Info("Partition :name for :table was created",
			args.Name(nextPartitionName), args.String{N: "table", V: database + table})
	}
	return err
}

// PartitionsInfo Print info about partitions
func PartitionsInfo(database, table string) error {
	parsed, exist, _ := db.InformSchema(database, table)

	// Table does not exist
	if !exist {
		return errors.New("Table '" + database + "." + table + " doesn't exist")
	}

	// Table is not partitioned
	if exist && len(parsed) == 0 {
		return errors.New("Table '" + database + "." + table + " is not partitioned")
	}

	// Print
	util.Print(
		"Name\tExpression\tRows\tCreatedAt\tTill\t",
		func(w *tabwriter.Writer) {
			for _, s := range parsed {
				_, _ = fmt.Fprintf(w,
					"%s\t%s\t%d\t%s\t%d\n",
					s.Name, s.Expr, s.Count, s.Cr, s.Desc)
			}
		})

	return nil
}

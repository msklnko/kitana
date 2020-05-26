package cmd

import (
	"errors"
	"github.com/msklnko/kitana/db"
	"github.com/msklnko/kitana/prt"
	"github.com/msklnko/kitana/util"
	"github.com/spf13/cobra"
	"regexp"
	"strconv"
	"strings"
)

var prtCount *regexp.Regexp

func init() {
	prtCount = regexp.MustCompile(`(?m)^\+\d*$`)
}

var prtCmd = &cobra.Command{
	Use:   "prt",
	Short: "Used either to obtain information about information_schema.partitions",
}

var prtAdd = &cobra.Command{
	Use:   "add",
	Short: "Add partition",
	Args: func(cmd *cobra.Command, args []string) error {
		switch l := len(args); l {
		case 0:
			return errors.New("missing arguments (table, name, limiter)." +
				" Also (+) with count of partitions could be used")
		case 1:
			var tbls = strings.Split(args[0], ".")
			if len(tbls) != 2 {
				return errors.New("invalid property, should be schema+table name")
			}
			if !db.CheckTablePresent(tbls[0], tbls[1]) {
				return errors.New("table " + args[0] + " does not exist")
			}
			return errors.New("partition name and limiter are missing" +
				" Also (+) with count of partitions could be used")
		case 2:
			if prtCount.MatchString(args[1]) {
				// Means that `alias` was used to add partitions //TODO
				return nil
			}
			return errors.New("limiter is missing")
		default:
			return nil
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		var tbls = strings.Split(args[0], ".")

		if len(args) == 2 {
			cnt, err := strconv.Atoi(args[1][1:len(args[1])])
			//TODO just +
			util.Er(err)
			prt.BatchAdd(tbls[0], tbls[1], cnt)
		} else {
			db.AddPartition(tbls[0], tbls[1], args[1], args[2])
		}

		show, err := cmd.Flags().GetBool("show")
		util.Er(err)

		if show {
			prt.PartitionsInfo(tbls[0], tbls[1])
		}
	},
}

var prtStatus = &cobra.Command{
	Use:     "status",
	Aliases: []string{"st", "info"},
	Short:   "Show info about partitions",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("table name is missing")
		}
		var tbls = strings.Split(args[0], ".")
		if len(tbls) != 2 {
			return errors.New("invalid property, should be schema+table name")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		var tbls = strings.Split(args[0], ".")
		prt.PartitionsInfo(tbls[0], tbls[1])
	},
}

var prtDrop = &cobra.Command{
	Use:     "drop",
	Aliases: []string{"rm"},
	Short:   "Drop partition",
	Args: func(cmd *cobra.Command, args []string) error {
		switch l := len(args); l {
		case 0:
			return errors.New("missing arguments (table, partition name)")
		case 1:
			var tbls = strings.Split(args[0], ".")
			if len(tbls) != 2 {
				return errors.New("invalid property, should be schema+table name")
			}
			if !db.CheckTablePresent(tbls[0], tbls[1]) {
				return errors.New("table " + args[0] + " does not exist")
			}
			return errors.New("partition name is missing")
		default:
			return nil
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		var tbls = strings.Split(args[0], ".")
		db.DropPartition(tbls[0], tbls[1], args[1])

		show, err := cmd.Flags().GetBool("show")
		util.Er(err)

		if show {
			prt.PartitionsInfo(tbls[0], tbls[1])
		}
	},
}

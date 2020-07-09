package cmd

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	s "strings"

	"github.com/msklnko/kitana/db"
	"github.com/msklnko/kitana/partition"
	"github.com/spf13/cobra"
)

var prtCount *regexp.Regexp

func init() {
	prtCount = regexp.MustCompile(`(?m)^\+\d*$`)

	prtCmd.AddCommand(prtStatus)
	prtCmd.AddCommand(prtAdd)
	prtCmd.AddCommand(prtDrop)
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
			var tables = s.Split(args[0], ".")
			if len(tables) != 2 {
				return errors.New("invalid property, should be schema+table name")
			}

			present, err := db.CheckTablePresent(tables[0], tables[1])
			if err != nil {
				return err
			}
			if !present {
				return errors.New("table " + args[0] + " does not exist")
			}
			return errors.New("partition name and limiter are missing" +
				" Also (+) with count of partitions could be used")
		case 2:
			if prtCount.MatchString(args[1]) {
				// Means that `alias` was used to add partitions //TODO
				return nil
			} else if args[1] == "next" {
				return nil
			}
			return errors.New("limiter is missing")
		default:
			return nil
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		var tables = s.Split(args[0], ".")

		limiter, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		err = db.AddPartitions(tables[0], tables[1], map[string]int64{args[1]: limiter})
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		show, err := cmd.Flags().GetBool("show")
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		if show {
			_ = partition.PartitionsInfo(tables[0], tables[1])
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
		var tables = s.Split(args[0], ".")
		if len(tables) != 2 {
			return errors.New("invalid property, should be schema+table name")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		var tables = s.Split(args[0], ".")
		err := partition.PartitionsInfo(tables[0], tables[1])
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
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
			var tables = s.Split(args[0], ".")
			if len(tables) != 2 {
				return errors.New("invalid property, should be schema+table name")
			}
			present, err := db.CheckTablePresent(tables[0], tables[1])
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
			if !present {
				return errors.New("table " + args[0] + " does not exist")
			}
			return errors.New("partition name is missing")
		default:
			return nil
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		var tables = s.Split(args[0], ".")
		err := db.DropPartition(tables[0], tables[1], []string{args[1]})
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		show, err := cmd.Flags().GetBool("show")
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		if show {
			err := partition.PartitionsInfo(tables[0], tables[1])
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
		}
	},
}

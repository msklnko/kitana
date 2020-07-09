package cmd

import (
	"errors"
	"fmt"
	"github.com/msklnko/kitana/db"
	"github.com/msklnko/kitana/partition"
	"github.com/spf13/cobra"
	"os"
	"strconv"
	s "strings"
)

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

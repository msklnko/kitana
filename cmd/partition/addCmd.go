package partition

import (
	"errors"
	"strconv"
	"strings"

	"github.com/msklnko/kitana/config"
	"github.com/msklnko/kitana/db"
	"github.com/msklnko/kitana/partition"
	"github.com/spf13/cobra"
)

var addCmd = &cobra.Command{
	Use:   "add",
	Short: "Add partition",
	Args: func(cmd *cobra.Command, args []string) error {
		switch l := len(args); l {
		case 0:
			return errors.New("missing arguments (table, name, limiter)." +
				" Also (+) with count of partitions could be used")
		case 1:
			var tables = strings.Split(args[0], ".")
			if len(tables) != 2 {
				return errors.New("invalid property, should be schema+table name")
			}

			connection, err := config.Connect()
			if err != nil {
				return err
			}

			present, err := db.CheckTablePresent(connection, tables[0], tables[1])
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
	RunE: func(cmd *cobra.Command, args []string) error {
		var tables = strings.Split(args[0], ".")

		limiter, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return err
		}

		connection, err := config.Connect()
		if err != nil {
			return err
		}

		err = db.AddPartitions(connection, tables[0], tables[1], map[string]int64{args[1]: limiter})
		if err != nil {
			return err
		}

		if show {
			_ = partition.PartitionsInfo(connection, tables[0], tables[1])
		}
		return nil
	},
}

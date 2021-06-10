package cmd

import (
	"errors"
	"github.com/mono83/xray"
	"github.com/msklnko/kitana/config"
	"github.com/msklnko/kitana/db"
	"github.com/spf13/cobra"
	"strings"
)

var indexCmd = &cobra.Command{
	Use:     "index",
	Aliases: []string{"updateIndex"},
	Short:   "Update primary index (example: `kitana index database.table column1,column2`)",
	Long:    commentRules,
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 2 {
			return errors.New("invalid arguments, should be like `kitana index table column1,column2`")
		}

		if len(strings.Split(args[1], ",")) == 0 {
			return errors.New("no index specified, should be like `kitana index table column1,column2`")
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		var arguments = strings.Split(args[0], ".")

		logger := xray.ROOT.Fork()
		logger.Info("Executing update primary index for " + arguments[1])
		connection, err := config.Connect()
		if err != nil {
			return err
		}

		err = db.AlterPrimaryIndex(connection, arguments[0], arguments[1], strings.Split(args[1], ","))
		if err == nil {
			logger.Info("Primary index for " + arguments[1] + " was updated")
		}

		return err
	},
}

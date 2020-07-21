package cmd

import (
	"errors"
	"github.com/msklnko/kitana/partition"
	"strings"

	"github.com/msklnko/kitana/config"
	"github.com/msklnko/kitana/db"
	"github.com/msklnko/kitana/definition"
	"github.com/spf13/cobra"
)

var commentShowCreate bool
var commentForcePartition bool
var commentRules = `Comment format: [GM:C:T:R:Rc] where  
    GM - identifier
    C - column name for partitioning 
    T - partitioning type, ml(monthly), dl(daily)
    R - retention policy - d(drop), n(none), b(backup)
    Rc - retention policy - old partition count`

var commentCmd = &cobra.Command{
	Use:     "comment",
	Aliases: []string{"addComment", "cmt"},
	Short:   "Add comment to provided table in supported format [GM:C:T:R:Rc]",
	Long:    commentRules,
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 2 {
			return errors.New("requires two arguments, table name and comment")
		}
		return nil
	},
	PreRunE: func(cmd *cobra.Command, args []string) error {
		argument := args[1]
		matchString := definition.CommentPattern.MatchString(argument)
		if !matchString {
			return errors.New("invalid comment format, should be [GM:C:T:R:Rc] \n" + commentRules)
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		var arguments = strings.Split(args[0], ".")

		connection, err := config.Connect()
		if err != nil {
			return err
		}

		if err := db.AlterComment(connection, arguments[0], arguments[1], args[1]); err != nil {
			return err
		}

		if commentForcePartition {
			err = partition.PartitionTable(connection, arguments[0], arguments[1], 3)
			if err != nil {
				return err
			}
		}

		if commentShowCreate {
			return db.ShowCreateTable(connection, arguments[0], arguments[1])
		}
		return nil
	},
}

func init() {
	commentCmd.Flags().BoolVarP(&commentShowCreate, "show", "s", false, "Show create table after alter")
	commentCmd.Flags().BoolVarP(&commentForcePartition, "forcePartition", "f", false, "Force table partitioning")
}

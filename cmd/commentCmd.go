package cmd

import (
	"errors"
	"strings"

	"github.com/msklnko/kitana/db"
	"github.com/msklnko/kitana/definition"
	"github.com/spf13/cobra"
)

var commentShowCreate bool

var commentCmd = &cobra.Command{
	Use:     "comment",
	Aliases: []string{"addComment", "cmt"},
	Short:   "Add comment to provided table in supported format [GM:C:T:R:Rc]",
	Long: `Comment format: [GM:C:T:R:Rc] where \n
		\tC - column name for partitioning\n 
		\tT - partitioning type, m for monthly\n" 
		\tR - retention policy - d (drop), n (none), b (backup)\n 
		\tRc - retention policy - d (drop), n (none), b (backup)\n`,
	Args: cobra.MinimumNArgs(2),
	PreRunE: func(cmd *cobra.Command, args []string) error {
		argument := args[1]
		matchString := definition.CommentPattern.MatchString(argument)
		if !matchString {
			return errors.New("invalid comment format, should be [GM:C:T:R:Rc]")
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		var arguments = strings.Split(args[0], ".")
		if err := db.AlterComment(arguments[0], arguments[1], args[1]); err != nil {
			return err
		}

		if commentShowCreate {
			return db.ShowCreateTable(arguments[0], arguments[1])
		}
		return nil
	},
}

func init() {
	commentCmd.Flags().BoolVarP(&commentShowCreate, "show", "s", false, "Show create table after alter")
}

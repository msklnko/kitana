package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/msklnko/kitana/db"
	"github.com/msklnko/kitana/definition"
	"github.com/spf13/cobra"
)

var alterCmtCmd = &cobra.Command{
	Use:     "cmt",
	Aliases: []string{"addComment"},
	Short:   "Add comment to provided table in supported format [GM:C:T:R:Rc]",
	Long: "Comment format: [GM:C:T:R:Rc] where \n" +
		"\tC - column name for partitioning\n" +
		"\tT - partitioning type, m for monthly\n" +
		"\tR - retention policy - d (drop), n (none), b (backup)\n" +
		"\tRc - retention policy - d (drop), n (none), b (backup)\n",
	Args: cobra.MinimumNArgs(2),
	PreRun: func(cmd *cobra.Command, args []string) {
		comment := args[1]
		matchString := definition.CommentPattern.MatchString(comment)
		if !matchString {
			fmt.Println("invalid comment format, should be [GM:C:T:R:Rc]")
			os.Exit(1)
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		var tbls = strings.Split(args[0], ".")
		err := db.AlterComment(tbls[0], tbls[1], args[1])

		value, err := cmd.Flags().GetBool("show")
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
		if value {
			err := db.ShowCreateTable(tbls[0], tbls[1])
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
		}
	},
}

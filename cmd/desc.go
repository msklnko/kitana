package cmd

import (
	"errors"
	"fmt"
	"github.com/msklnko/kitana/db"
	"github.com/spf13/cobra"
	"strings"
)

var descCmd = &cobra.Command{
	Use:   "desc",
	Short: "Used either to obtain information about table structure",
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
		value, err := cmd.Flags().GetBool("comment")
		if err != nil {
			fmt.Println(err)
		}

		if value {
			fmt.Println("Connect with mysql")
		} else {
			var tbls = strings.Split(args[0], ".")
			db.Desc(tbls[0], tbls[1])
			fmt.Println("without flag", err)
		}
	},
}

var showCmd = &cobra.Command{
	Use: "show",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("missed schema name")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		db.ShowTables(args[0])
	},
}

var alterCmtCmd = &cobra.Command{
	Use:     "cmt",
	Aliases: []string{"addComment"},
	Short:   "Add comment to provided table in supported format [GM:C:T:R:Rc]",
	Long: "Comment format: [GM:C:T:R:Rc] where \n" +
		"\tC - column name for partitioning\n" +
		"\tT - partitioning type, m for monthly\n" +
		"\tR - retention policy - d (drop), n (none), b (backup)\n" +
		"\tR - retention policy - d (drop), n (none), b (backup)\n",
	Args: func(cmd *cobra.Command, args []string) error {
		cobra.RangeArgs(2, 2)
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		var tbls = strings.Split(args[0], ".")
		db.AlterComment(tbls[0], tbls[1], args[1])
	},
}

func init() {
	// Add command
	KitanaCmd.AddCommand(descCmd)
	descCmd.Flags().BoolP("comment", "c", false, "Show table comment")

	KitanaCmd.AddCommand(showCmd)

	KitanaCmd.AddCommand(alterCmtCmd)
}

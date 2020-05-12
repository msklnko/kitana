package cmd

import (
	"errors"
	"fmt"
	"github.com/msklnko/kitana/db"
	"github.com/spf13/cobra"
	"os"
	"regexp"
	"strings"
)

// Show all tables from db with partition configs
var showCmd = &cobra.Command{
	Use:   "show",
	Short: "Show all tables",
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

// Add comment to table
var cmtPattern *regexp.Regexp

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
		cmt := args[1]
		matchString := cmtPattern.MatchString(cmt)
		if !matchString {
			fmt.Println("invalid comment format, should be [GM:C:T:R:Rc]")
			os.Exit(1)
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		var tbls = strings.Split(args[0], ".")
		db.AlterComment(tbls[0], tbls[1], args[1])

		value, err := cmd.Flags().GetBool("show")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if value {
			db.ShowCreateTable(tbls[0], tbls[1])
		}
	},
}

func init() {
	// Regexp for comment
	cmtPattern = regexp.MustCompile(`(?m)^\[GM:\w+:(m|d):(d|n|b):\d\]$`)
}

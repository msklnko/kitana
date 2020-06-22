package cmd

import (
	"github.com/spf13/cobra"
)

// KitanaCmd main command
var KitanaCmd = &cobra.Command{}

func init() {
	KitanaCmd.AddCommand(prtCmd)
	prtCmd.AddCommand(prtStatus)
	prtCmd.AddCommand(prtAdd)
	prtCmd.AddCommand(prtDrop)
	prtCmd.PersistentFlags().BoolP("show", "s", false, "Show partitions")

	KitanaCmd.AddCommand(showCmd)
	showCmd.Flags().BoolP("partitioned", "p", false, "Show only partitioned tables")
	showCmd.Flags().BoolP("comment", "c", false, "Show only commented tables")
	showCmd.Flags().BoolP("definition", "d", false, "Show comment definition")

	KitanaCmd.AddCommand(alterCmtCmd)
	alterCmtCmd.Flags().BoolP("show", "s", false, "Show create table")

	KitanaCmd.AddCommand(demon)

	KitanaCmd.AddCommand(testCmd)
}

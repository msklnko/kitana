package cmd

import (
	"github.com/spf13/cobra"
)

var KitanaCmd = &cobra.Command{}

func init() {
	// Add command
	KitanaCmd.AddCommand(prtCmd)
	prtCmd.AddCommand(prtStatus)
	prtCmd.AddCommand(prtAdd)
	//prtCmd.Flags().BoolP("show", "s", false, "Show partitions")
	//prtCmd.Flags().StringP("create", "c", "", "Create partition")

	KitanaCmd.AddCommand(showCmd)
	KitanaCmd.AddCommand(alterCmtCmd)
	alterCmtCmd.Flags().BoolP("show", "s", false, "Show create table")
}

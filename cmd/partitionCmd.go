package cmd

import (
	"github.com/spf13/cobra"
)

var KitanaCmd = &cobra.Command{}

func init() {
	// Add command
	KitanaCmd.AddCommand(partCmd)
	//descCmd.Flags().BoolP("show", "s", true, "Show create table")

	KitanaCmd.AddCommand(showCmd)
	KitanaCmd.AddCommand(alterCmtCmd)
	alterCmtCmd.Flags().BoolP("show", "s", false, "Show create table")
}

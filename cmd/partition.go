package cmd

import (
	"github.com/spf13/cobra"
	"regexp"
)

var prtCount *regexp.Regexp

func init() {
	prtCount = regexp.MustCompile(`(?m)^\+\d*$`)

	prtCmd.AddCommand(prtStatus)
	prtCmd.AddCommand(prtAdd)
	prtCmd.AddCommand(prtDrop)
}

var prtCmd = &cobra.Command{
	Use:   "prt",
	Short: "Used either to obtain information about information_schema.partitions",
}

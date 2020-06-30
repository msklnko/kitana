package cmd

import (
	"errors"
	"fmt"

	"github.com/msklnko/kitana/definition"
	"github.com/spf13/cobra"
)

var testCmd = &cobra.Command{
	Use:   "test pattern",
	Short: "Tests given string as table comment",
	RunE: func(cmd *cobra.Command, a []string) error {
		if len(a) != 1 {
			return errors.New("exactly one argument expected")
		}

		def, err := definition.Parse(a[0])
		if err != nil {
			return err
		}

		fmt.Println("Given:            ", a[0])
		fmt.Println("Partition Type:   ", def.PartitionType)
		fmt.Println("Retention Policy: ", def.Rp)
		fmt.Println("Column:           ", def.Column)
		fmt.Println("Count:            ", def.Count)
		return nil
	},
}

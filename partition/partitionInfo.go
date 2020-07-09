package partition

import (
	"errors"
	"fmt"
	"text/tabwriter"

	"github.com/mono83/xray"
	"github.com/mono83/xray/args"
	"github.com/msklnko/kitana/db"
	"github.com/msklnko/kitana/definition"
	"github.com/msklnko/kitana/util"
)

// ShowTables Show tables
// comment - show only commented table
// part - show only partitioned tables
// def - print comment definition
func ShowTables(database string, comment, part, def bool, logger xray.Ray) error {
	tables, err := db.ShowTables(database, comment, part)

	if err != nil {
		return err
	}

	// Print
	if len(tables) > 0 {
		util.Print(util.Ternary(def, "Database\tName\tComment\tDefinition\t", "Database\tName\tComment\t"),
			func(w *tabwriter.Writer) {
				for _, s := range tables {
					if def {
						var parsed = s.Comment
						if len(s.Comment) > 0 {
							shelved, err := definition.Parse(s.Comment)
							if err != nil {
								logger.Warning("Comment for table :name could not be parsed", args.Name(s.Name))
							} else {
								parsed = shelved.String()
							}
						}
						_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", s.Database, s.Name, s.Comment, parsed)
					} else {
						_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", s.Database, s.Name, s.Comment)
					}
				}
			})
		fmt.Println("[", database, "] Count :", len(tables))
	}
	return nil
}

// PartitionsInfo Print info about partitions
func PartitionsInfo(database, table string) error {
	parsed, exist, _, err := db.InformSchema(database, table)
	if err != nil {
		return err
	}

	// Table does not exist
	if !exist {
		return errors.New("Table '" + database + "." + table + " doesn't exist")
	}

	// Table is not partitioned
	if exist && len(parsed) == 0 {
		return errors.New("Table '" + database + "." + table + " is not partitioned")
	}

	// Print
	util.Print(
		"Name\tExpression\tTill\t",
		func(w *tabwriter.Writer) {
			for _, partition := range parsed {
				_, _ = fmt.Fprintf(w,
					"%s\t%s\t%d\n",
					partition.Name, partition.Expression, partition.Limiter)
			}
		})

	return nil
}

// CheckStatus Check inconsistency (partitioned = comment)
func CheckStatus(database string) error {
	return nil
}

package partition

import (
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
		util.Print(util.Ternary(def, "Name\tComment\tDefinition\t", "Name\tComment\t"),
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
						_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", s.Name, s.Comment, parsed)
					} else {
						_, _ = fmt.Fprintf(w, "%s\t%s\n", s.Name, s.Comment)
					}
				}
			})
		fmt.Println("[", database, "] Count :", len(tables))
	}
	return nil
}

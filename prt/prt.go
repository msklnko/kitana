package prt

import (
	"errors"
	"fmt"
	"github.com/msklnko/kitana/cmt"
	"github.com/msklnko/kitana/db"
	"github.com/msklnko/kitana/util"
	"sort"
	"text/tabwriter"
	"time"
)

// BatchAdd Add provided count of partitions
func BatchAdd(sh, tb string, count int) {
	partitions, exist, comment := db.InformSchema(sh, tb)

	if !exist {
		util.Er(errors.New("table " + sh + "." + tb + " doesn't exist\n"))
		return
	}

	if len(partitions) == 0 {
		util.Er(errors.New("table " + sh + "." + tb + " is not partitioned\n"))
		return
	}

	if !cmt.CommentPattern.MatchString(comment) {
		util.Er(errors.New("in order to partition the table " + sh + "." + tb + " need to add a comment, " +
			"use `kitana cmt add [GM:C:T:R:Rc]`"))
		return
	}

	// Sort partitions (just in case)
	sort.SliceStable(partitions, func(i, j int) bool {
		return partitions[i].Desc < partitions[j].Desc
	})

	// Last partition, identifier to start
	lastPartition := partitions[len(partitions)-1]
	lastDateLimitter := time.Unix(lastPartition.Desc, 0)
	lastName := lastPartition.Name

	// Candidates
	candidates := make(map[string]int64)

	// Comment
	def, _ := cmt.Def(comment)
	rp = def.Rp
	if(rp.)


	db.AddPartitions(sh, tb, candidates)

	//TODO Retention policy
}

// PartitionsInfo Print info about partitions
func PartitionsInfo(sh, tb string) {
	parsed, exist, _ := db.InformSchema(sh, tb)

	// Table does not exist
	if !exist {
		fmt.Printf("Table '%s' doesn't exist\n", sh+"."+tb)
		return
	}

	// Table is not partitioned
	if exist && len(parsed) == 0 {
		fmt.Printf("Table '%s' is not partitioned\n", sh+"."+tb)
		return
	}

	// Print
	util.Print(
		"Name\tExpression\tRows\tCreatedAt\tTill\t",
		func(w *tabwriter.Writer) {
			for _, s := range parsed {
				_, _ = fmt.Fprintf(w,
					"%s\t%s\t%d\t%s\t%d\n",
					s.Name, s.Expr, s.Count, s.Cr, s.Desc)
			}
		})

}

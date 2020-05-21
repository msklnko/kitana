package util

import (
	"fmt"
	"os"
	"regexp"
	"text/tabwriter"
)

// CmtPattern Partitioned comment pattern
var CmtPattern *regexp.Regexp
var PartIdentification string = "GM"

func init() {
	// Regexp for comment
	CmtPattern = regexp.MustCompile(`(?m)^\[GM:\w+:(m|d):(d|n|b):\d\]$`)
}

// Er Print error and stop application
func Er(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func Print(headers string, fn Iterate) {
	w := new(tabwriter.Writer)
	// Format in tab-separated columns with a tab stop of 8.
	w.Init(os.Stdout, 0, 8, 0, '\t', 0)
	_, _ = fmt.Fprintln(w, headers)

	fn(w)
	_ = w.Flush()
}

type Iterate func(*tabwriter.Writer)

func Ternar(condition bool, case1, case2 string) string {
	if condition {
		return case1
	} else {
		return case2
	}
}

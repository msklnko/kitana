package util

import (
	"fmt"
	"os"
	"text/tabwriter"
)

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

func Ternary(condition bool, case1, case2 string) string {
	if condition {
		return case1
	} else {
		return case2
	}
}

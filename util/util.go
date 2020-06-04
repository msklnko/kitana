package util

import (
	"fmt"
	"os"
	"text/tabwriter"
)

// Print Represents data as table
func Print(headers string, fn Iterate) {
	w := new(tabwriter.Writer)
	// Format in tab-separated columns with a tab stop of 8.
	w.Init(os.Stdout, 0, 8, 0, '\t', 0)
	_, _ = fmt.Fprintln(w, headers)

	fn(w)
	_ = w.Flush()
}

// Iterate Function should be implemented to describe how to print values from Print
type Iterate func(*tabwriter.Writer)

// Ternary Ternary operator, instead of if else
func Ternary(condition bool, case1, case2 string) string {
	if condition {
		return case1
	}
	return case2
}

// Contains Searches string in slice
func Contains(data []string, requested string) bool {
	for _, record := range data {
		if record == requested {
			return true
		}
	}
	return false
}

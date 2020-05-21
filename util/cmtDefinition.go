package util

import "strings"

func Def(cmt string) string {

	if !CmtPattern.MatchString(cmt) {
		return "comment " + cmt + " did not match with partitioning rules"
	}

	_ = strings.Split(cmt[1:len(cmt)-1], ":")
	return ""
}

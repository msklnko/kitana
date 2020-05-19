package util

import (
	"fmt"
	"os"
)

// Er Print error and stop application
func Er(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

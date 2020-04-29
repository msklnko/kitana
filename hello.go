package main

import (
	"fmt"
	"github.com/msklnko/kitana/cmd"
	"os"
)

func init() {
}

func main() {
	if err := cmd.KitanaCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

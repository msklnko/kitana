package main

import (
	"github.com/mono83/xray/std/xcobra"
	"github.com/msklnko/kitana/cmd"
)

func main() {
	// Attaching XRay logging and starting command
	xcobra.Start(cmd.KitanaCmd)
}

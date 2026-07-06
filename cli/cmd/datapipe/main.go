// Command datapipe is the DataPipe CLI (API-130). Increment 0 only ships a
// version subcommand; flow/deploy/import-export commands land alongside the
// control-plane REST API in later increments.
package main

import (
	"fmt"
	"os"
)

const version = "0.0.0-dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println("datapipe " + version)
		return
	}
	fmt.Fprintln(os.Stderr, "usage: datapipe version")
	os.Exit(1)
}

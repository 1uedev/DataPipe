// Command datapipe is the DataPipe CLI (API-130). Increment 2 adds "deploy",
// which embeds the flow engine directly (ARC-130 all-in-one style) to load,
// validate, and run a flow file locally; deploying through the control
// plane's REST API + runtime gRPC channel lands in Increment 3.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/1uedev/DataPipe/engine/flow"

	_ "github.com/1uedev/DataPipe/engine/nodes/busin"
	_ "github.com/1uedev/DataPipe/engine/nodes/busout"
	_ "github.com/1uedev/DataPipe/engine/nodes/calculator"
	_ "github.com/1uedev/DataPipe/engine/nodes/convert"
	_ "github.com/1uedev/DataPipe/engine/nodes/debuglog"
	_ "github.com/1uedev/DataPipe/engine/nodes/delay"
	_ "github.com/1uedev/DataPipe/engine/nodes/errortrigger"
	_ "github.com/1uedev/DataPipe/engine/nodes/filewatch"
	_ "github.com/1uedev/DataPipe/engine/nodes/filter"
	_ "github.com/1uedev/DataPipe/engine/nodes/httpin"
	_ "github.com/1uedev/DataPipe/engine/nodes/httprequest"
	_ "github.com/1uedev/DataPipe/engine/nodes/httpresponse"
	_ "github.com/1uedev/DataPipe/engine/nodes/inject"
	_ "github.com/1uedev/DataPipe/engine/nodes/lookup"
	_ "github.com/1uedev/DataPipe/engine/nodes/loop"
	_ "github.com/1uedev/DataPipe/engine/nodes/merge"
	_ "github.com/1uedev/DataPipe/engine/nodes/mqttin"
	_ "github.com/1uedev/DataPipe/engine/nodes/mqttout"
	_ "github.com/1uedev/DataPipe/engine/nodes/schedule"
	_ "github.com/1uedev/DataPipe/engine/nodes/script"
	_ "github.com/1uedev/DataPipe/engine/nodes/set"
	_ "github.com/1uedev/DataPipe/engine/nodes/splitbatch"
	_ "github.com/1uedev/DataPipe/engine/nodes/sqlsink"
	_ "github.com/1uedev/DataPipe/engine/nodes/sqlsource"
	_ "github.com/1uedev/DataPipe/engine/nodes/state"
	_ "github.com/1uedev/DataPipe/engine/nodes/stoperror"
	_ "github.com/1uedev/DataPipe/engine/nodes/switchroute"
	_ "github.com/1uedev/DataPipe/engine/nodes/template"
	_ "github.com/1uedev/DataPipe/engine/nodes/trycatch"
)

const version = "0.0.0-dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	var err error
	switch os.Args[1] {
	case "version":
		fmt.Println("datapipe " + version)
	case "deploy":
		err = runDeploy(os.Args[2:])
	case "backup":
		err = runBackup(os.Args[2:])
	default:
		usage()
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: datapipe version | datapipe deploy <flow.json> [-for <duration>] | datapipe backup export|restore ...")
}

func runDeploy(args []string) error {
	fs := flag.NewFlagSet("deploy", flag.ContinueOnError)
	forDuration := fs.Duration("for", 0, "stop automatically after this duration (0 = run until interrupted)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: datapipe deploy <flow.json> [-for <duration>]")
	}
	path := fs.Arg(0)

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}
	f, err := flow.Parse(data)
	if err != nil {
		return fmt.Errorf("parsing %s: %w", path, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *forDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *forDuration)
		defer cancel()
	}

	dep := flow.NewDeployment(slog.Default())
	if err := dep.Deploy(ctx, f); err != nil {
		return fmt.Errorf("deploy: %w", err)
	}
	slog.Info("flow deployed", "id", f.ID, "nodes", len(f.Graph.Nodes))

	<-ctx.Done()
	slog.Info("stopping")
	dep.Stop()
	return nil
}

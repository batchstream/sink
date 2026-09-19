package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/liran/sink/internal/app"
	"github.com/liran/sink/internal/config"
	"github.com/liran/sink/internal/gateway"
	"github.com/liran/sink/internal/logging"
)

var version = "dev"

func main() {
	err := executeCommand(os.Args[1:], os.Stdout, os.Stderr)
	if err != nil {
		var reported *reportedError
		if !errors.As(err, &reported) {
			slog.Error("sink stopped", "error", err)
		}
		os.Exit(1)
	}
}

func executeCommand(args []string, stdout io.Writer, stderr io.Writer) error {
	if len(args) > 0 {
		switch args[0] {
		case "version":
			if len(args) != 1 {
				return errors.New("sink version does not accept arguments")
			}
			_, err := fmt.Fprintln(stdout, version)
			return err
		case "dlq":
			return runDeadLetterCommand(args[1:], stdout, stderr)
		case "lua":
			return runLuaCommand(args[1:], stdout, stderr)
		case "config":
			return runConfigCommand(args[1:], stdout)
		}
	}
	configPath, err := parseConfigPath(args)
	if err != nil {
		return err
	}
	return run(configPath)
}

func runConfigCommand(args []string, stdout io.Writer) error {
	if len(args) == 0 || args[0] != "check" {
		return errors.New("usage: sink config check --config FILE")
	}
	configPath, err := parseConfigPath(args[1:])
	if err != nil {
		return err
	}
	loaded, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if loaded.Mode == config.ModeGateway {
		if err := gateway.ValidateRoutes(loaded.Gateway.Routes); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintln(stdout, "Configuration schema and limits are valid.")
	return err
}

func parseConfigPath(args []string) (string, error) {
	flags := flag.NewFlagSet("sink", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "path to the YAML configuration file")
	if err := flags.Parse(args); err != nil {
		return "", fmt.Errorf("parse command arguments: %w", err)
	}
	if flags.NArg() != 0 {
		return "", fmt.Errorf("unexpected command argument %q", flags.Arg(0))
	}
	trimmed := strings.TrimSpace(*configPath)
	if trimmed == "" {
		return "", errors.New("--config is required")
	}
	return trimmed, nil
}

type reportedError struct{ error }

func run(configPath string) (runErr error) {
	loaded, err := config.Load(configPath)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logOptions := logging.Options{Config: loaded.Logging, Role: string(loaded.Mode), Store: loaded.Storage.Name, Version: version}
	logs, err := logging.New(ctx, logOptions)
	if err != nil {
		return err
	}
	previous := slog.Default()
	logs.Install()
	defer func() {
		if runErr != nil {
			slog.Error("Sink stopped after an unrecoverable error", "component", "runtime", "event", "process_failed", "error_type", logging.ErrorType(runErr))
			runErr = &reportedError{error: runErr}
		} else {
			slog.Info("Sink stopped", "component", "runtime", "event", "process_stopped")
		}
		logs.Close()
		slog.SetDefault(previous)
	}()

	options := app.Options{Config: loaded, Version: version}
	running, err := app.New(ctx, options)
	if err != nil {
		return err
	}
	defer running.Close()
	slog.Info("Starting Sink", "component", "runtime", "event", "process_started")
	return running.Run(ctx)
}

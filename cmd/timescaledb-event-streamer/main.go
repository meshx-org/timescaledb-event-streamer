/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"fmt"
	"github.com/meshx-org/timescaledb-event-streamer/internal"
	"github.com/meshx-org/timescaledb-event-streamer/internal/erroring"
	"github.com/meshx-org/timescaledb-event-streamer/internal/logging"
	"github.com/meshx-org/timescaledb-event-streamer/internal/sysconfig"
	"github.com/meshx-org/timescaledb-event-streamer/internal/telemetry"
	"github.com/meshx-org/timescaledb-event-streamer/internal/waiting"
	spiconfig "github.com/meshx-org/timescaledb-event-streamer/spi/config"
	"github.com/meshx-org/timescaledb-event-streamer/spi/version"
	"github.com/urfave/cli"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

var (
	configurationFile string
	verbose           bool
	withCaller        bool
	logToStdErr       bool
	versionOnly       bool
	profiling         bool
)

func main() {
	app := &cli.App{
		Name:  "timescaledb-event-streamer",
		Usage: "CDC (Change Data Capture) for TimescaleDB Hypertable",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "config,c",
				Value:       "",
				Usage:       "Load configuration from `FILE`",
				Destination: &configurationFile,
			},
			&cli.BoolFlag{
				Name:        "verbose",
				Usage:       "Show verbose output",
				Destination: &verbose,
			},
			&cli.BoolFlag{
				Name:        "caller",
				Usage:       "Collect caller information for log messages",
				Destination: &withCaller,
			},
			&cli.BoolFlag{
				Name:        "log-to-stderr",
				Usage:       "Redirects logging output to stderr, necessary when using StdOut as the sink",
				Destination: &logToStdErr,
			},
			&cli.BoolFlag{
				Name:        "version",
				Usage:       "Prints the version and exits",
				Destination: &versionOnly,
			},
			&cli.BoolFlag{
				Name:        "profiling",
				Usage:       "Enables the Go profiler",
				Destination: &profiling,
			},
		},
		Action: start,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func start(
	ctx *cli.Context,
) error {

	log := os.Stdout
	if logToStdErr {
		log = os.Stderr
	}
	fmt.Fprintln(log, ctx.App.Usage)
	fmt.Fprintf(log, "%s version %s (git revision %s; branch %s)\n",
		version.BinName, version.Version, version.CommitHash, version.Branch,
	)

	if versionOnly {
		return nil
	}

	if profiling {
		go func() {
			if err := http.ListenAndServe("localhost:8080", nil); err != nil {
				fmt.Fprintf(log, "Failed to initialize the profiler. %+v\n", err)
			}
		}()
	}

	logging.WithCaller = withCaller
	logging.WithVerbose = verbose

	config := &spiconfig.Config{}

	// No configuration file set? Try env variable!
	if configurationFile == "" {
		if cf, present := os.LookupEnv("TIMESCALEDB_EVENT_STREAMER_CONFIG"); present {
			fmt.Fprintln(log, "Using configuration file from environment variable")
			configurationFile = cf
		}
	}

	if configurationFile != "" {
		fmt.Fprintf(log, "Loading configuration file: %s\n", configurationFile)
		f, err := os.Open(configurationFile)
		if err != nil {
			return cli.NewExitError(fmt.Sprintf("Configuration file couldn't be opened: %v\n", err), 3)
		}

		b, err := io.ReadAll(f)
		if err != nil {
			return cli.NewExitError(fmt.Sprintf("Configuration file couldn't be read: %v\n", err), 4)
		}

		tomlConfig := filepath.Ext(strings.ToLower(configurationFile)) == ".toml"
		if err := spiconfig.Unmarshall(b, config, tomlConfig); err != nil {
			return cli.NewExitError(fmt.Sprintf("Configuration file couldn't be decoded: %v\n", err), 5)
		}
	}

	if err := logging.InitializeLogging(config, logToStdErr); err != nil {
		return err
	}

	telemetryShutdown, err := telemetry.Initialize(config)
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("Telemetry initialization failed: %v\n", err), 7)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := telemetryShutdown(shutdownCtx); err != nil {
			fmt.Fprintf(log, "Error during telemetry shutdown: %v\n", err)
		}
	}()

	if spiconfig.GetOrDefault(config, spiconfig.PropertyPostgresqlConnection, "") == "" {
		return cli.NewExitError("PostgreSQL connection string required", 6)
	}

	systemConfig := sysconfig.NewSystemConfig(config)
	// NewStreamer returns a concrete *cli.ExitError; keep it in its own variable
	// so the nil check is on the pointer and not on the error interface (which
	// would always be non-nil for a typed-nil value).
	streamer, exitErr := internal.NewStreamer(systemConfig)
	if exitErr != nil {
		return exitErr
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	done := waiting.NewWaiter()
	go func() {
		<-signals
		if err := streamer.Stop(); err != nil {
			fmt.Fprintf(log, "Hard error when stopping replication: %v\n", err)
			os.Exit(1)
		}
		done.Signal()
	}()

	if err := streamer.Start(); err != nil {
		if err2 := streamer.Stop(); err2 != nil {
			fmt.Fprintf(log, "Error during early shutdown: %v\n", err2)
		}
		return err
	}

	if err := done.Await(); err != nil {
		return erroring.AdaptError(err, 10)
	}

	return nil
}

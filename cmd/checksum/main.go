package main

import (
	"fmt"
	"github.com/arnkore/rds-checksum/pkg/checksum"
	"github.com/arnkore/rds-checksum/pkg/metadata"
	"github.com/arnkore/rds-checksum/pkg/storage" // Assuming storage package exists
	"github.com/jessevdk/go-flags"
	"io"
	"log/slog"
	"os"

	_ "github.com/go-sql-driver/mysql" // Assuming MySQL for results DB
)

type Options struct {
	// --- Flags for Source DB ---
	SourceUser     string `long:"source-user" description:"Username for source database" required:"true"`
	SourcePassword string `long:"source-password" description:"Password for source database" required:"true"`
	SourceHost     string `long:"source-host" description:"Hostname for source database" required:"true"`
	SourcePort     int    `long:"source-port" description:"Port for source database" required:"true"`
	SourceDB       string `long:"source-db" description:"Database name for source database" required:"true"`
	// --- Flags for Target DB ---
	TargetUser     string `long:"target-user" description:"Username for target database" required:"true"`
	TargetPassword string `long:"target-password" description:"Password for target database" required:"true"`
	TargetHost     string `long:"target-host" description:"Hostname for target database" required:"true"`
	TargetPort     int    `long:"target-port" description:"Port for target database" required:"true"`
	TargetDB       string `long:"target-db" description:"Database name for target database" required:"true"`
	// --- Flags for Results DB ---
	ResultUser     string `long:"result-user" description:"Username for result database" required:"true"`
	ResultPassword string `long:"result-password" description:"Password for result database" required:"true"`
	ResultHost     string `long:"result-host" description:"Hostname for result database" required:"true"`
	ResultPort     int    `long:"result-port" description:"Port for result database" required:"true"`
	ResultDB       string `long:"result-db" description:"Database name for result database" required:"true"`

	TableName       string `long:"table-name" description:"Name of the table to validate" required:"true"`
	RowsPerBatch    int    `long:"rows-per-batch" default:"100000" description:"Target number of rows to process in each batch/batch" required:"false"`
	ConcurrentLimit int    `long:"concurrent_limit" default:"10" description:"Number of tasks to concurrently processing" required:"false"`
	LogFile         string `long:"log-file" description:"Path to the log file. If not specified, logs only to console." required:"false"`
}

func main() {
	// --- Processing flags ---
	opts := processFlags()

	// --- Setup Logger ---
	logger := setupLogger(opts.LogFile)

	// --- Connect to Results DB ---
	resultDbConnProvider := metadata.NewDBConnProvider(&metadata.Config{
		Host:     opts.ResultHost,
		Port:     opts.ResultPort,
		User:     opts.ResultUser,
		Password: opts.ResultPassword,
		Database: opts.ResultDB,
	})
	resultsDB, err := resultDbConnProvider.CreateDbConn()
	if err != nil {
		slog.Error("Error connecting to metadata database", "error", err)
		os.Exit(1)
	}
	dbStore := storage.NewStore(resultsDB)

	// --- Create and run the checksum ---
	slog.Info("Starting checksum validation", "source_ip", opts.SourceHost, "source_port", opts.SourcePort, "source_db", opts.SourceDB,
		"target_ip", opts.TargetHost, "target_port", opts.TargetPort, "target_db", opts.TargetDB,
		"table", opts.TableName, "batch_size", opts.RowsPerBatch, "concurrency", opts.ConcurrentLimit)
	srcConfig := metadata.NewConfig(opts.SourceHost, opts.SourcePort, opts.SourceUser, opts.SourcePassword, opts.SourceDB)
	targetConfig := metadata.NewConfig(opts.TargetHost, opts.TargetPort, opts.TargetUser, opts.TargetPassword, opts.TargetDB)
	validator := checksum.NewChecksumValidator(logger, srcConfig, targetConfig, opts.TableName, opts.RowsPerBatch, opts.ConcurrentLimit, dbStore)
	result, runErr := validator.Run()

	// --- Handle Results ---
	jobID := validator.GetJobID() // Assume validator exposes the job ID it created

	if runErr != nil {
		// Error occurred *during* validation run (potentially after job created)
		slog.Error("Validation job run failed and potentially incomplete", "job_id", jobID, "error", runErr)
		os.Exit(1)
	}

	// If we reached here, Run completed its process (though checksums might mismatch)
	// Results are stored in DB, provide summary log
	if result.Match {
		slog.Info("✅ Result: Checksums match.", "job_id", jobID)
	} else {
		slog.Warn("❌ Result: Checksums DO NOT match.", "job_id", jobID)
	}
	os.Exit(0)
}

func processFlags() Options {
	var opts Options
	parser := flags.NewParser(&opts, flags.Default)
	_, err := parser.Parse()
	if err != nil {
		// Use fmt for initial flag parsing errors before logger is set up
		fmt.Fprintf(os.Stderr, "error parsing flags: %v\n", err)
		os.Exit(1)
	}
	return opts
}

func setupLogger(logFile string) *slog.Logger {
	var logWriter io.Writer = os.Stderr // Default to stderr for errors/info before file check
	if logFile != "" {
		logFile, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0660)
		if err != nil {
			// Still use fmt here as logger setup failed
			fmt.Fprintf(os.Stderr, "error opening log file %s: %v\n", logFile, err)
			os.Exit(1)
		}
		defer logFile.Close()
		// Use MultiWriter to write to both file and stderr
		logWriter = io.MultiWriter(os.Stderr, logFile)
	}

	// Configure slog logger
	logLevel := new(slog.LevelVar) // Defaults to Info
	logger := slog.New(slog.NewTextHandler(logWriter, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)
	return logger
}

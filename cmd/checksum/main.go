package main

import (
	"fmt"
	"github.com/arnkore/rds-checksum/pkg/checksum"
	"github.com/arnkore/rds-checksum/pkg/metadata"
	"github.com/arnkore/rds-checksum/pkg/storage" // Assuming storage package exists
	"github.com/jessevdk/go-flags"
	"io"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql" // Assuming MySQL for results DB
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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
	setupLogger(opts.LogFile)

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
		log.Error().Err(err).Msg("Error connecting to metadata database")
		os.Exit(1)
	}
	dbStore := storage.NewStore(resultsDB)

	// --- Create and run the checksum ---
	log.Info().Str("source_ip", opts.SourceHost).
		Int("source_port", opts.SourcePort).
		Str("source_db", opts.SourceDB).
		Str("target_ip", opts.TargetHost).
		Int("target_port", opts.TargetPort).
		Str("target_db", opts.TargetDB).
		Str("table", opts.TableName).
		Int("batch_size", opts.RowsPerBatch).
		Int("concurrency", opts.ConcurrentLimit).
		Msg("Starting checksum validation")
	srcConfig := metadata.NewConfig(opts.SourceHost, opts.SourcePort, opts.SourceUser, opts.SourcePassword, opts.SourceDB)
	targetConfig := metadata.NewConfig(opts.TargetHost, opts.TargetPort, opts.TargetUser, opts.TargetPassword, opts.TargetDB)
	validator := checksum.NewChecksumValidator(srcConfig, targetConfig, opts.TableName, opts.RowsPerBatch, opts.ConcurrentLimit, dbStore)
	result, runErr := validator.Run()

	// --- Handle Results ---
	jobID := validator.GetJobID() // Assume validator exposes the job ID it created

	if runErr != nil {
		// Error occurred *during* validation run (potentially after job created)
		log.Error().Err(runErr).Int64("job_id", jobID).Msg("Validation job run failed and potentially incomplete")
		os.Exit(1)
	}

	// If we reached here, Run completed its process (though checksums might mismatch)
	// Results are stored in DB, provide summary log
	if result.Match {
		log.Info().Int64("job_id", jobID).Msg("✅ Result: Checksums match.")
	} else {
		log.Warn().Int64("job_id", jobID).Msg("❌ Result: Checksums DO NOT match.")
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

func setupLogger(logFile string) {
	var consoleWriter io.Writer = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339Nano} // Default to pretty stderr
	if logFile != "" {
		logF, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0660)
		if err != nil {
			log.Fatal().Err(err).Str("file", logFile).Msg("Error opening log file")
		}
		// 配置 ConsoleWriter 输出到文件
		fileWriter := zerolog.ConsoleWriter{
			Out:        logF,
			TimeFormat: time.RFC3339Nano, // 带毫秒的时间格式
			NoColor:    true,             // 禁用颜色（文件不需要）
		}
		// Use MultiWriter to write to both file (plain JSON) and console (pretty)
		consoleWriter = zerolog.MultiLevelWriter(consoleWriter, fileWriter)
	}

	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"
	log.Logger = zerolog.New(consoleWriter).With().Timestamp().Logger() // Set as global logger
}

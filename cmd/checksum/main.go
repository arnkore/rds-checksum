package main

import (
	"context"
	"fmt"
	"github.com/arnkore/rds-checksum/pkg/checksum"
	"github.com/arnkore/rds-checksum/pkg/common"
	"github.com/arnkore/rds-checksum/pkg/metadata"
	"github.com/arnkore/rds-checksum/pkg/storage" // Assuming storage package exists
	"github.com/jessevdk/go-flags"
	"io"
	"os"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql" // Assuming MySQL for results DB
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	GET_TABLE_INFO_MAX_RETRY_TIMES = 3
)

type Options struct {
	// --- Flags for Source DB ---
	SourceUser     string `long:"source_user" description:"Username for source database" required:"true"`
	SourcePassword string `long:"source_password" description:"Password for source database" required:"true"`
	SourceHost     string `long:"source_host" description:"Hostname for source database" required:"true"`
	SourcePort     int    `long:"source_port" description:"Port for source database" required:"true"`
	SourceDB       string `long:"source_db" description:"Database name for source database" required:"true"`
	// --- Flags for Target DB ---
	TargetUser     string `long:"target_user" description:"Username for target database" required:"true"`
	TargetPassword string `long:"target_password" description:"Password for target database" required:"true"`
	TargetHost     string `long:"target_host" description:"Hostname for target database" required:"true"`
	TargetPort     int    `long:"target_port" description:"Port for target database" required:"true"`
	TargetDB       string `long:"target_db" description:"Database name for target database" required:"true"`
	// --- Flags for Results DB ---
	ResultUser     string `long:"result_user" description:"Username for result database" required:"true"`
	ResultPassword string `long:"result_password" description:"Password for result database" required:"true"`
	ResultHost     string `long:"result_host" description:"Hostname for result database" required:"true"`
	ResultPort     int    `long:"result_port" description:"Port for result database" required:"true"`
	ResultDB       string `long:"result_db" description:"Database name for result database" required:"true"`

	TableName       string `long:"table_name" description:"Name of the table to validate" required:"true"`
	RowsPerBatch    int    `long:"rows_per_batch" default:"100000" description:"Target number of rows to process in each batch/batch" required:"false"`
	ConcurrentLimit int    `long:"concurrent_limit" default:"10" description:"Number of tasks to concurrently processing" required:"false"`
	CalcCrc32InDB   bool   `long:"calc_crc32_in_db" description:"Calculate checksum in MySQL using CRC32 function" required:"false"`
	LogFile         string `long:"log_file" description:"Path to the log file. If not specified, logs only to console." required:"false"`
}

func main() {
	// --- Processing flags ---
	opts := processFlags()
	// --- Setup Logger ---
	setupLogger(opts.LogFile)
	// --- Connect to Results DB ---
	dbStore := setUpStorage(opts)

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
	validator := checksum.NewChecksumValidator(srcConfig, targetConfig, opts.TableName, opts.RowsPerBatch, opts.ConcurrentLimit, opts.CalcCrc32InDB, dbStore)

	// 非重试的checksum task是否运行结束
	var firstRunChecksumFinished = &atomic.Bool{}
	var normalCancelFlag = &atomic.Bool{}
	// 使用 context 控制是否继续重试
	ctx, cancelFunc := context.WithCancel(context.Background())
	go setupChecksumRetryTask(ctx, cancelFunc, validator, firstRunChecksumFinished, normalCancelFlag)
	result, runErr := validator.Run()
	firstRunChecksumFinished.Store(true)
	jobID := validator.GetJobID()

	if runErr != nil {
		log.Error().Err(runErr).Int64(common.JOB_ID_KEY, jobID).Msg("Validation job run failed and potentially incomplete")
		// FIXME 有报错，要尝试重试，而不是直接退出程序。
		os.Exit(1)
	}

	// If we reached here, Run completed its process (though checksums might mismatch)
	if result.Match {
		checksum.NormalCancel(cancelFunc, normalCancelFlag)
	}

	for {
		select {
		case <-ctx.Done():
			if normalCancelFlag.Load() {
				log.Info().Int64(common.JOB_ID_KEY, jobID).Msg("✅ Result: Checksums match.")
			} else {
				log.Warn().Int64(common.JOB_ID_KEY, jobID).Msg("❌ Result: Checksums DO NOT match.")
			}
			return
		}
	}
}

func processFlags() *Options {
	var opts Options
	parser := flags.NewParser(&opts, flags.Default)
	_, err := parser.Parse()
	if err != nil {
		// Use fmt for initial flag parsing errors before logger is set up
		fmt.Fprintf(os.Stderr, "error parsing flags: %v\n", err)
		os.Exit(1)
	}
	return &opts
}

func setupLogger(logFile string) {
	var logWriter io.Writer = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339Nano} // Default to pretty stderr
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
		logWriter = zerolog.MultiLevelWriter(logWriter, fileWriter)
	}

	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = zerolog.New(logWriter).With().Timestamp().Logger() // Set as global logger
}

func setUpStorage(opts *Options) *storage.Store {
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
	return dbStore
}

func setupChecksumRetryTask(ctx context.Context, cancelFunc context.CancelFunc, validator *checksum.ChecksumValidator,
	firstRunChecksumFinished, normalCancelFlag *atomic.Bool) {
	ticker := time.NewTicker(common.CHECKSUM_RETRY_INTERVAL)
	defer ticker.Stop()

	srcTableInfo := getSrcTableInfo(validator)
	if srcTableInfo == nil {
		checksum.AbnormalCancel(cancelFunc, normalCancelFlag)
		return
	}

	for {
		select {
		case <-ticker.C:
			validator.RetryRowChecksum(cancelFunc, firstRunChecksumFinished, normalCancelFlag, srcTableInfo)
		case <-ctx.Done():
			return
		}
	}
}

func getSrcTableInfo(validator *checksum.ChecksumValidator) *metadata.TableInfo {
	var srcTableInfo *metadata.TableInfo
	var err error
	for i := 0; i < GET_TABLE_INFO_MAX_RETRY_TIMES; i++ {
		srcTableInfo, err = validator.GetSrcTableMetaProvider().QueryTableInfo(validator.TableName)
		if err != nil {
			log.Error().Err(err).Str("table", validator.TableName).Msg("Failed to get source table info")
			if i == GET_TABLE_INFO_MAX_RETRY_TIMES-1 {
				return nil
			}
		}
	}
	return srcTableInfo
}

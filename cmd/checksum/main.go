package main

import (
	"context"
	"fmt"
	"github.com/arnkore/rds-checksum/pkg/checksum"
	"github.com/arnkore/rds-checksum/pkg/common"
	"github.com/arnkore/rds-checksum/pkg/metadata"
	"github.com/arnkore/rds-checksum/pkg/storage" // Assuming storage package exists
	"github.com/jessevdk/go-flags"
	"os"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql" // Assuming MySQL for results DB
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
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
	log.WithFields(log.Fields{
		"source_ip":   opts.SourceHost,
		"source_port": opts.SourcePort,
		"source_db":   opts.SourceDB,
		"target_ip":   opts.TargetHost,
		"target_port": opts.TargetPort,
		"target_db":   opts.TargetDB,
		"table":       opts.TableName,
		"batch_size":  opts.RowsPerBatch,
		"concurrency": opts.ConcurrentLimit,
	}).Info("Starting checksum validation")
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
		log.WithError(runErr).WithField(common.JOB_ID_KEY, jobID).Error("Validation job run failed and potentially incomplete")
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
				log.WithField(common.JOB_ID_KEY, jobID).Info("✅ Result: Checksums match.")
			} else {
				log.WithField(common.JOB_ID_KEY, jobID).Warn("❌ Result: Checksums DO NOT match.")
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

func setupLogger(logFilePath string) {
	// 创建 Logrus 实例
	logger := log.New()
	// 设置日志级别（生产环境建议 Info 或 Warn）
	logger.SetLevel(log.InfoLevel)
	// 设置日志格式
	logger.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,                      // 显示完整时间戳
		TimestampFormat: "2006-01-02 15:04:05.000", // 标准时间格式
		ForceColors:     false,                     // 生产环境禁用颜色（控制台）
		DisableQuote:    true,                      // 避免字段值被引号包裹
		PadLevelText:    true,                      // 统一日志级别宽度
	})

	// 设置日志备份&滚动策略
	logFile := &lumberjack.Logger{
		Filename:   logFilePath, // 日志文件路径
		MaxSize:    100,         // 单个日志文件最大 100MB
		MaxBackups: 10,          // 保留最近 5 个备份
		MaxAge:     30,          // 保留 30 天的日志
		Compress:   true,        // 压缩旧日志
	}

	// 多输出：同时写入文件和控制台
	logger.SetOutput(os.Stdout)                 // 控制台输出
	logger.AddHook(&fileHook{logFile: logFile}) // 文件输出钩子

	// 添加全局字段（上下文信息）
	logger.WithFields(log.Fields{
		"app":     "rds-checksum",
		"version": "1.0.0",
	}).Info("Logrus logger initialized")
}

// 自定义 Hook 用于文件输出
type fileHook struct {
	logFile *lumberjack.Logger
}

func (h *fileHook) Levels() []log.Level {
	return log.AllLevels
}

func (h *fileHook) Fire(entry *log.Entry) error {
	line, err := entry.String()
	if err != nil {
		return err
	}
	_, err = h.logFile.Write([]byte(line))
	return err
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
		log.WithError(err).Error("Error connecting to metadata database")
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
			log.WithError(err).WithField("table", validator.TableName).Error("Failed to get source table info")
			if i == GET_TABLE_INFO_MAX_RETRY_TIMES-1 {
				return nil
			}
		}
	}
	return srcTableInfo
}

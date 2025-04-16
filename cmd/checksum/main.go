package main

import (
	"fmt"
	"github.com/arnkore/rds-checksum/pkg/checksum"
	"github.com/arnkore/rds-checksum/pkg/metadata"
	"github.com/arnkore/rds-checksum/pkg/storage" // Assuming storage package exists
	"github.com/jessevdk/go-flags"
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
}

func main() {
	var opts Options
	parser := flags.NewParser(&opts, flags.Default)
	_, err := parser.Parse()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	// --- Connect to Results DB ---
	var resultConfig = &metadata.Config{
		Host:     opts.ResultHost,
		Port:     opts.ResultPort,
		User:     opts.ResultUser,
		Password: opts.ResultPassword,
		Database: opts.ResultDB,
	}
	resultDbConnProvider := metadata.NewDBConnProvider(resultConfig)
	resultsDB, err := resultDbConnProvider.CreateDbConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to metadata database: %v\n", err)
		os.Exit(1)
	}
	dbStore := storage.NewStore(resultsDB)

	// --- Create and run the validator ---
	srcConfig := metadata.NewConfig(opts.SourceHost, opts.SourcePort, opts.SourceUser, opts.SourcePassword, opts.SourceDB)
	targetConfig := metadata.NewConfig(opts.TargetHost, opts.TargetPort, opts.TargetUser, opts.TargetPassword, opts.TargetDB)
	validator := checksum.NewChecksumValidator(srcConfig, targetConfig, opts.TableName, opts.RowsPerBatch, opts.ConcurrentLimit, dbStore)
	result, runErr := validator.RunValidation()

	// --- Handle Results ---
	jobID := validator.GetJobID() // Assume validator exposes the job ID it created

	if runErr != nil {
		// Error occurred *during* validation run (potentially after job created)
		fmt.Fprintf(os.Stderr, "Validation run failed: %v\n", runErr)
		if jobID > 0 {
			fmt.Fprintf(os.Stderr, "Job ID: %d. Check database for details.", jobID)
		}
		os.Exit(1)
	}

	// If we reached here, RunValidation completed its process (though checksums might mismatch)
	HandleChecksumResult(jobID, result)
}

func HandleChecksumResult(jobID int64, result *checksum.CompareChecksumResults) {
	if jobID > 0 {
		fmt.Printf("Validation process completed for Job ID: %d", jobID)
		if result.Match {
			fmt.Println("✅ Result: Checksums match.")
		} else {
			fmt.Println("❌ Result: Checksums DO NOT match.")
		}
		fmt.Println("Detailed results stored in the database.")
		if result.Match {
			os.Exit(0)
		} else {
			os.Exit(1) // Exit with error status for mismatch
		}
	} else {
		// Results DB not used, print summary to console as before
		if result.Match {
			fmt.Println("--------------------------------------------------")
			fmt.Println("✅ Validation Successful: Checksums match.")
			fmt.Printf("   Source Rows: %d\n", result.SrcTotalRows)
			fmt.Printf("   Target Rows: %d\n", result.TargetTotalRows)
			fmt.Println("--------------------------------------------------")
			os.Exit(0)
		} else {
			fmt.Println("--------------------------------------------------")
			fmt.Println("❌ Validation Failed: Checksums DO NOT match.")
			if result.RowCountMismatch {
				fmt.Printf("   Row count mismatch: Source=%d, Target=%d\n", result.SrcTotalRows, result.TargetTotalRows)
			}
			if len(result.MismatchBatches) > 0 {
				fmt.Printf("   Mismatched batches indices: %v\n", result.MismatchBatches)
			}
			if result.SrcError != nil || result.TargetError != nil {
				fmt.Printf("   Errors during processing: Source Err=%v, Target Err=%v\n", result.SrcError, result.TargetError)
			}
			fmt.Println("--------------------------------------------------")
			os.Exit(1)
		}
	}
}

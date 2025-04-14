package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/arnkore/rds-checksum/pkg/checksum"
	"github.com/arnkore/rds-checksum/pkg/metadata"
	"github.com/arnkore/rds-checksum/pkg/storage" // Assuming storage package exists

	_ "github.com/go-sql-driver/mysql" // Assuming MySQL for results DB
)

// Function to construct DSN from parts
func buildDSN(user, password, host string, port int, dbname string) string {
	// Format: username:password@protocol(address)/dbname?param=value
	// Ensure necessary parameters for the driver (e.g., parseTime)
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		user, password, host, port, dbname)
}

func main() {
	// --- Flags for Source DB ---
	sourceUser := flag.String("source-user", "", "Username for source database")
	sourcePassword := flag.String("source-password", "", "Password for source database")
	sourceHost := flag.String("source-host", "", "Host or IP address for source database")
	sourcePort := flag.Int("source-port", 3306, "Port number for source database")
	sourceDB := flag.String("source-db", "", "Database name for source database")

	// --- Flags for Target DB ---
	targetUser := flag.String("target-user", "", "Username for target database")
	targetPassword := flag.String("target-password", "", "Password for target database")
	targetHost := flag.String("target-host", "", "Host or IP address for target database")
	targetPort := flag.Int("target-port", 3306, "Port number for target database")
	targetDB := flag.String("target-db", "", "Database name for target database")

	// --- Flags for Results DB ---
	resultsDBUser := flag.String("results-db-user", "", "Username for results database")
	resultsDBPassword := flag.String("results-db-password", "", "Password for results database")
	resultsDBHost := flag.String("results-db-host", "", "Host or IP address for results database")
	resultsDBPort := flag.Int("results-db-port", 3306, "Port number for results database")
	resultsDBName := flag.String("results-db-name", "", "Database name for results database")
	initializeSchema := flag.Bool("initialize-schema", false, "If set, try to create necessary tables in results DB")

	// --- Common flags ---
	tableName := flag.String("table", "", "Name of the table to validate")
	rowsPerBatch := flag.Int("rows-per-batch", 1000, "Target number of rows to process in each batch/partition")

	flag.Parse()

	// --- Validate required flags ---
	missingFlags := []string{}
	if *sourceUser == "" {
		missingFlags = append(missingFlags, "--source-user")
	}
	if *sourceHost == "" {
		missingFlags = append(missingFlags, "--source-host")
	}
	if *sourceDB == "" {
		missingFlags = append(missingFlags, "--source-db")
	}
	if *targetUser == "" {
		missingFlags = append(missingFlags, "--target-user")
	}
	if *targetHost == "" {
		missingFlags = append(missingFlags, "--target-host")
	}
	if *targetDB == "" {
		missingFlags = append(missingFlags, "--target-db")
	}
	if *tableName == "" {
		missingFlags = append(missingFlags, "--table")
	}
	// Validate results DB flags if specified
	if *resultsDBHost != "" || *resultsDBName != "" { // If using results db, need host and name
		if *resultsDBHost == "" {
			missingFlags = append(missingFlags, "--results-db-host")
		}
		if *resultsDBName == "" {
			missingFlags = append(missingFlags, "--results-db-name")
		}
		// User/Pass might be optional depending on DB setup
	}

	if len(missingFlags) > 0 {
		fmt.Fprintf(os.Stderr, "Error: Missing required flags: %s\n", strings.Join(missingFlags, ", "))
		flag.Usage()
		os.Exit(1)
	}

	if *rowsPerBatch <= 0 {
		fmt.Fprintln(os.Stderr, "Error: --rows-per-batch must be a positive integer.")
		os.Exit(1)
	}

	// --- Connect to Results DB ---
	var resultsDB *sql.DB
	var dbStore *storage.Store
	var err error
	if *resultsDBHost != "" { // Only connect if results DB host is provided
		resultsDSN := buildDSN(*resultsDBUser, *resultsDBPassword, *resultsDBHost, *resultsDBPort, *resultsDBName)
		fmt.Printf("Connecting to results database: %s@%s:%d/%s\n", *resultsDBUser, *resultsDBHost, *resultsDBPort, *resultsDBName)
		resultsDB, err = sql.Open("mysql", resultsDSN) // Assuming mysql driver
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error connecting to results database: %v\n", err)
			os.Exit(1)
		}
		defer resultsDB.Close()

		err = resultsDB.Ping()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error pinging results database: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Successfully connected to results database.")

		dbStore = storage.NewStore(resultsDB)

		// Optionally initialize schema
		if *initializeSchema {
			fmt.Println("Initializing database schema...")
			err = dbStore.InitializeSchema()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error initializing schema: %v\n", err)
				os.Exit(1)
			}
			fmt.Println("Schema initialization attempted.")
		}
	} else {
		fmt.Println("Results database not configured. Results will only be printed to console.")
	}

	// --- Prepare Source/Target Config ---
	srcConfig := metadata.Config{
		Host:     *sourceHost,
		Port:     *sourcePort,
		User:     *sourceUser,
		Password: *sourcePassword,
		Database: *sourceDB,
	}
	targetConfig := metadata.Config{
		Host:     *targetHost,
		Port:     *targetPort,
		User:     *targetUser,
		Password: *targetPassword,
		Database: *targetDB,
	}

	// --- Create and run the validator ---
	// The validator now needs the storage handler
	validator := checksum.NewUnifiedChecksumValidator(srcConfig, targetConfig, *tableName, *rowsPerBatch, dbStore)

	fmt.Printf("Starting checksum validation for table '%s' with batches of approximately %d rows...\n", *tableName, *rowsPerBatch)
	fmt.Printf("Source: %s@%s:%d/%s\n", *sourceUser, *sourceHost, *sourcePort, *sourceDB)
	fmt.Printf("Target: %s@%s:%d/%s\n", *targetUser, *targetHost, *targetPort, *targetDB)

	// RunValidation will now handle storing results if dbStore is not nil
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
			if len(result.MismatchPartitions) > 0 {
				fmt.Printf("   Mismatched partitions indices: %v\n", result.MismatchPartitions)
			}
			if result.SrcError != nil || result.TargetError != nil {
				fmt.Printf("   Errors during processing: Source Err=%v, Target Err=%v\n", result.SrcError, result.TargetError)
			}
			fmt.Println("--------------------------------------------------")
			os.Exit(1)
		}
	}
}

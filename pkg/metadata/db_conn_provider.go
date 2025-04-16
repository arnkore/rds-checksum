package metadata

import (
	"database/sql"
	"fmt"
)

type DbConnProvider struct {
	databaseName string
	dsn          string
}

// NewDBConnProvider creates a new DbConnProvider.
func NewDBConnProvider(config *Config) *DbConnProvider {
	dsn := config.GenerateDSN()
	return &DbConnProvider{databaseName: config.Database, dsn: dsn}
}

// CreateDbConn creates a new connection
func (p *DbConnProvider) CreateDbConn() (*sql.DB, error) {
	db, err := sql.Open("mysql", p.dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	return db, nil
}

// Close closes the database connection.
func (p *DbConnProvider) Close(db *sql.DB) error {
	if db != nil {
		return db.Close()
	}
	return nil
}

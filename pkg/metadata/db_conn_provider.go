package metadata

import (
	"database/sql"
	"fmt"
)

type DbConnProvider struct {
	databaseName string
	dbConn       *sql.DB
}

// NewDBConnProvider creates a new DbConnProvider.
func NewDBConnProvider(config *Config) (*DbConnProvider, error) {
	dsn := config.GenerateDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	return &DbConnProvider{databaseName: config.Database, dbConn: db}, nil
}

// Close closes the database connection.
func (p *DbConnProvider) Close() error {
	if p.dbConn != nil {
		return p.dbConn.Close()
	}
	return nil
}

func (p *DbConnProvider) GetDbConn() *sql.DB {
	return p.dbConn
}

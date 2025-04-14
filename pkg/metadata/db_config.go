package metadata

import "fmt"

// Config holds the MySQL connection configuration
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

func NewConfig(host string, port int, user string, password string, database string) *Config {
	return &Config{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		Database: database,
	}
}

// GenerateDSN returns the Data Source Name for the MySQL connection
func (config *Config) GenerateDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", config.User, config.Password, config.Host, config.Port, config.Database)
}

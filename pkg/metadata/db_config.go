package metadata

// Config holds the MySQL connection configuration
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

func NewConfig() *Config {
	return &Config{
		Host: config.Host,
		Port: config.Port,
		User: config.User,
		Password: config.Password,
		Database: config.Database,
	}
}

// GenerateDSN returns the Data Source Name for the MySQL connection
func (config *Config) GenerateDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", config.User, config.Password, config.Host, config.Port, config.Database)
}

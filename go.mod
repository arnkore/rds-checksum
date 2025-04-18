module github.com/arnkore/rds-checksum

go 1.23.0

toolchain go1.23.8

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/enriquebris/goconcurrentqueue v0.7.0
	github.com/go-sql-driver/mysql v1.9.1
	github.com/jessevdk/go-flags v1.6.1
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.10.0
	golang.org/x/sync v0.13.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/sys v0.21.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/arnkore/rds-checksum => ./

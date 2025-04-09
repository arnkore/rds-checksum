module github.com/liuzonghao/mychecksum

go 1.23.0

toolchain go1.23.8

require (
	github.com/go-sql-driver/mysql v1.9.1
	github.com/stretchr/testify v1.10.0
	golang.org/x/sync v0.13.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/liuzonghao/mychecksum => ./

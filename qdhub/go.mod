module qdhub

go 1.24.2

require (
	github.com/google/uuid v1.6.0
	github.com/jmoiron/sqlx v1.4.0
	github.com/mattn/go-sqlite3 v1.14.32
	gopkg.in/yaml.v3 v3.0.1
)

replace github.com/LENAX/task-engine => ../../task-engine

require (
	github.com/LENAX/task-engine v1.0.3 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
)

package migrate

import (
	"fmt"
	"os"
	"strings"

	migrate "github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/go-pg/pg/extra/pgdebug"
	"github.com/go-pg/pg/v10"
)

func Migrate(opType string, migrationPath string) error {
	addr, password := GetAddrAndPassword()
	port := 5432

	// Base64 strings can contain '/' characters, which mess up URL parsing.
	// So we substitute it with a URL-friendly character.
	password = strings.ReplaceAll(password, "/", "%2f")

	m, err := migrate.New(
		migrationPath,
		fmt.Sprintf("postgresql://postgres:%s@%s:%v/postgres?sslmode=disable", password, addr, port))
	if err != nil {
		return fmt.Errorf("unable to connect to DB: %v", err)
	}

	if opType == "" {
		// applies every migrations till the lastest migration-sql present.
		// Automatically makes sure about the version the current database is on and updates it.
		if err := m.Up(); err != nil && err != migrate.ErrNoChange {
			return fmt.Errorf("SEVERE: migration could not be applied; %v", err)
		}
		return nil

	} else if opType == "drop_smtable" {
		dbq, err := ConnectToDatabaseWithPort(true, "postgres", port)
		if err != nil {
			return fmt.Errorf("unable to connect to DB: %v", err)
		} else {
			_, err = dbq.Exec("DROP TABLE schema_migrations")
			if err != nil {
				return fmt.Errorf("unable to Drop table: %v", err)
			}
		}
		return nil

	} else if opType == "drop" {
		if err := m.Drop(); err != nil {
			return fmt.Errorf("unable to Drop DB: %v", err)
		}
		return nil

	} else if opType == "downgrade_migration" {
		if err := m.Steps(-1); err != nil {
			return fmt.Errorf("unable to downgrade migration version by 1 level: %v", err)
		}
		return nil
	} else if opType == "upgrade_migration" {
		if err := m.Steps(1); err != nil {
			return fmt.Errorf("unable to upgrade migration version by 1 level: %v", err)
		}
		return nil
	} else {
		return fmt.Errorf("invalid argument passed")
	}

}

func GetAddrAndPassword() (string, string) {
	addr := "localhost"
	if isEnvExist("DB_ADDR") {
		addr = os.Getenv("DB_ADDR")
	}

	password := "gitops"
	if isEnvExist("DB_PASS") {
		password = os.Getenv("DB_PASS")
	}
	return addr, password
}

// connectToDatabaseWithPort connects to Postgres with a defined port
func ConnectToDatabaseWithPort(verbose bool, dbName string, port int) (*pg.DB, error) {
	addr, password := GetAddrAndPassword()
	opts := &pg.Options{
		Addr:     fmt.Sprintf("%s:%v", addr, port),
		User:     "postgres",
		Password: password,
		Database: dbName,
	}

	db := pg.Connect(opts)

	if err := checkConn(db); err != nil {
		return nil, fmt.Errorf("%v, unable to connect to database: Host:'%s' User:'%s' Pass:'%s' DB:'%s' ", err, opts.Addr, opts.User, opts.Password, opts.Database)
	}

	if verbose {
		db.AddQueryHook(pgdebug.DebugHook{
			// Print all queries.
			Verbose: true,
		})
	}

	return db, nil
}

func isEnvExist(key string) bool {
	if _, ok := os.LookupEnv(key); ok {
		return true
	}

	return false
}

func checkConn(db *pg.DB) error {
	var n int
	_, err := db.QueryOne(pg.Scan(&n), "SELECT 1")
	return err
}

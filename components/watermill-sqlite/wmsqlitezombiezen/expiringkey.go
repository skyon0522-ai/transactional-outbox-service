package wmsqlitezombiezen

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type expiringKeyRepository struct {
	Connection      *sqlite.Conn
	StmtInsert      *sqlite.Stmt
	StmtCleanUp     *sqlite.Stmt
	Expiration      time.Duration
	CleanUpInterval time.Duration
	NextCleanUp     time.Time
}

// ExpiringKeyRepositoryConfiguration intializes the expiring key repository in [NewExpiringKeyRepository] constructor.
type ExpiringKeyRepositoryConfiguration struct {
	// Connection is SQLite3 database handle. This connection must not be shared.
	Connection *sqlite.Conn

	// TableName is the name of the table used to store expiring keys.
	// The
	// Defaults to "watermill_expiring_keys".
	TableName string

	// Expiration is the duration after which a key is considered expired.
	// If lower than five milliseconds, it is set to five milliseconds.
	// Defaults to one minute.
	Expiration time.Duration

	// CleanUpInterval is the interval at which expired keys are cleaned up.
	// Defaults to 15 seconds.
	CleanUpInterval time.Duration
}

// NewExpiringKeyRepository creates a repository that tracks key duplicates within a certain time frame.
// Cleans up expired keys periodically at an interval when checking for duplicates. Does not create a go routine.
// Use as a configuration option for [middleware.Deduplicator].
func NewExpiringKeyRepository(config ExpiringKeyRepositoryConfiguration) (_ middleware.ExpiringKeyRepository, finalizer func() error, err error) {
	if config.Connection == nil {
		return nil, nil, ErrDatabaseConnectionIsNil
	}
	if config.TableName == "" {
		config.TableName = "watermill_expiring_keys"
	} else if err = validateTopicName(config.TableName); err != nil {
		return nil, nil, fmt.Errorf("table name does not match topic name rules: %w", err)
	}
	if config.Expiration < time.Millisecond*5 {
		config.Expiration = time.Millisecond * 5
	}
	if config.CleanUpInterval == 0 {
		config.CleanUpInterval = 15 * time.Second
	}

	if err = sqlitex.ExecuteTransient(
		config.Connection,
		`CREATE TABLE IF NOT EXISTS '`+config.TableName+`' (
			key TEXT PRIMARY KEY NOT NULL,
			expires_at INTEGER NOT NULL
		);`,
		nil); err != nil {
		return nil, nil, fmt.Errorf("untable to create %q SQLite table: %w", config.TableName, err)
	}

	r := &expiringKeyRepository{
		Connection:      config.Connection,
		Expiration:      config.Expiration,
		CleanUpInterval: config.CleanUpInterval,
	}

	r.StmtInsert, err = r.Connection.Prepare(`INSERT INTO '` + config.TableName + `' (key, expires_at) VALUES (?, ?)`)
	if err != nil {
		return nil, nil, fmt.Errorf("untable to prepare clean up statement for %q SQLite table: %w", config.TableName, err)
	}
	r.StmtCleanUp, err = r.Connection.Prepare(`DELETE FROM '` + config.TableName + `' WHERE expires_at<?`)
	if err != nil {
		_ = r.StmtInsert.Finalize()
		return nil, nil, fmt.Errorf("untable to prepare clean up statement for %q SQLite table: %w", config.TableName, err)
	}

	return r, func() error {
		return errors.Join(r.StmtInsert.Finalize(), r.StmtCleanUp.Finalize())
	}, nil
}

func (r *expiringKeyRepository) IsDuplicate(ctx context.Context, key string) (ok bool, err error) {
	t := time.Now()
	if err = r.CleanUp(t); err != nil {
		return true, err
	}

	r.StmtInsert.BindText(1, key)
	r.StmtInsert.BindInt64(2, t.Add(r.Expiration).UnixNano())
	_, err = r.StmtInsert.Step()
	if err != nil {
		if sqlite.ErrCode(err) == sqlite.ResultConstraintPrimaryKey {
			return true, nil
		}
		return true, errors.Join(err, r.StmtInsert.Reset())
	}
	return false, r.StmtInsert.Reset()
}

func (r *expiringKeyRepository) CleanUp(until time.Time) (err error) {
	if until.Before(r.NextCleanUp) {
		return nil // not yet
	}
	r.NextCleanUp = until.Add(r.CleanUpInterval)
	r.StmtCleanUp.BindInt64(1, until.UnixNano())
	_, err = r.StmtCleanUp.Step()
	if err != nil {
		return errors.Join(err, r.StmtCleanUp.Reset())
	}
	return r.StmtCleanUp.Reset()
}

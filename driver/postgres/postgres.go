// Package postgres implements PostgreSQL 12+ owner-checked lease operations for
// package anylock using database/sql. Callers own the pool and choose a PostgreSQL
// SQL driver that honors operation contexts and network deadlines. database/sql
// cannot impose a deadline on a SQL driver that ignores cancellation.
//
// New neither connects nor runs DDL. Use EnsureSchema explicitly, or provision
// the documented table through migrations. Expiry uses the database wall clock;
// expired rows are logically absent and need not be deleted before acquisition.
// Maintenance/cleanup of unused expired rows is the caller's responsibility.
//
// Mutations are single atomic statements. Renewal checks owner and fresh expiry
// after locking the row, so a row-lock wait cannot revive an expired lease.
// Release deletes a matching owner's row regardless of expiry; its atomic owner
// check protects successors. TTLs round up to microseconds. The driver does not
// start workers, close connections, or strengthen failover guarantees. Optional
// fencing uses a permanent ascending CACHE 1 sequence, allocated after acquisition.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tr1v3r/anylock"
)

const createSQL = `CREATE TABLE IF NOT EXISTS %s (
    key text COLLATE pg_catalog."C" PRIMARY KEY,
    owner text COLLATE pg_catalog."C" NOT NULL,
    expires_at timestamp with time zone NOT NULL
)`

const acquireSQL = `INSERT INTO %s AS existing (key, owner, expires_at)
VALUES ($1, $2, pg_catalog.clock_timestamp() + $3::pg_catalog.interval)
ON CONFLICT (key) DO UPDATE
SET owner = EXCLUDED.owner, expires_at = pg_catalog.clock_timestamp() + $3::pg_catalog.interval
WHERE existing.expires_at <= pg_catalog.clock_timestamp()`

// Check held's owner and expiry only after FOR UPDATE returns the locked row.
// Keep the expiry predicate outside the CTE so it runs after any lock wait.
// MATERIALIZED makes the boundary explicit; FOR UPDATE also prevents inlining.
const renewSQL = `WITH held AS MATERIALIZED (
    SELECT key, owner, expires_at FROM %s WHERE key = $1 FOR UPDATE
)
UPDATE %[1]s AS target
SET expires_at = pg_catalog.clock_timestamp() + $3::pg_catalog.interval
FROM held
WHERE target.key = held.key AND held.owner = $2 AND held.expires_at > pg_catalog.clock_timestamp()`

const releaseSQL = `DELETE FROM %s WHERE key = $1 AND owner = $2`

// Driver implements anylock.Driver over a shared permanent PostgreSQL table.
// Construct it with New; its zero value is not usable. It is safe for concurrent
// use. Only *sql.DB is accepted, not an externally managed long-lived transaction.
type Driver struct {
	db       *sql.DB
	prefix   string
	table    string
	sequence string
}

var _ anylock.Driver = (*Driver)(nil)

// Option configures a Driver before it is shared.
type Option func(*Driver) error

// WithTable selects a permanent table, either table or schema.table. Unqualified
// names use public, not search_path. Each component must be an ASCII identifier
// (letter/underscore followed by letters/digits/underscores), at most 63 bytes.
// Names are quoted and case-sensitive; the schema must already exist.
func WithTable(name string) Option {
	return func(d *Driver) error {
		table, err := quoteTable(name)
		if err != nil {
			return err
		}
		d.table = table
		return nil
	}
}

// New constructs a driver without connecting or creating schema. The default
// table is public.anylock_leases. Prefix is prepended verbatim to logical keys;
// reserve a namespace and include separators as desired. Nil pools/options and
// prefixes containing NUL or invalid UTF-8 are rejected. Callers retain pool ownership.
func New(db *sql.DB, prefix string, options ...Option) (*Driver, error) {
	if db == nil {
		return nil, errors.New("anylock/postgres: nil database")
	}
	if !validText(prefix) {
		return nil, errors.New("anylock/postgres: prefix must be valid UTF-8 without NUL")
	}
	d := &Driver{db: db, prefix: prefix, table: `"public"."anylock_leases"`, sequence: `"public"."anylock_fences"`}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("anylock/postgres: nil option")
		}
		if err := option(d); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// EnsureSchema explicitly creates the table if absent. It neither creates a
// schema nor migrates/validates an existing table. Prefer a serialized migration
// for production startup; concurrent CREATE TABLE IF NOT EXISTS can still race
// in PostgreSQL's catalogs. The table must not have triggers or external writers
// that change the lock protocol. Like other calls, this operation is bounded.
func (d *Driver) EnsureSchema(ctx context.Context) error {
	if err := d.validateContext(ctx); err != nil {
		return err
	}
	_, err := d.exec(ctx, "schema", fmt.Sprintf(createSQL, d.table))
	return err
}

// TryAcquire atomically reserves an absent/expired key. An unexpired row is busy
// even if its owner matches. Data values are bound parameters, never SQL text.
func (d *Driver) TryAcquire(
	ctx context.Context, key, owner string, opts anylock.AcquireOptions,
) (anylock.AcquireResult, error) {
	if err := d.validate(ctx, key, owner); err != nil {
		return anylock.AcquireResult{}, err
	}
	interval, err := ttlInterval(opts.TTL)
	if err != nil {
		return anylock.AcquireResult{}, err
	}
	if opts.Fencing {
		return d.acquireFenced(ctx, key, owner, interval)
	}
	ok, err := d.exec(ctx, "acquire", fmt.Sprintf(acquireSQL, d.table), d.prefix+key, owner, interval)
	return anylock.AcquireResult{Acquired: ok}, err
}

// Renew atomically extends only the unexpired matching owner by ttl, measured
// using the database wall clock. A row-lock wait is followed by a fresh expiry
// check, not one cached at statement/transaction start. False means ownership loss.
func (d *Driver) Renew(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	if err := d.validate(ctx, key, owner); err != nil {
		return false, err
	}
	interval, err := ttlInterval(ttl)
	if err != nil {
		return false, err
	}
	return d.exec(ctx, "renew", fmt.Sprintf(renewSQL, d.table), d.prefix+key, owner, interval)
}

// Release atomically deletes a matching owner's row, even after expiry. True
// means a row was deleted; absent or successor-owned rows return false, nil.
// The owner check also protects successors installed during a row-lock wait.
func (d *Driver) Release(ctx context.Context, key, owner string) (bool, error) {
	if err := d.validate(ctx, key, owner); err != nil {
		return false, err
	}
	return d.exec(ctx, "release", fmt.Sprintf(releaseSQL, d.table), d.prefix+key, owner)
}

func (d *Driver) exec(ctx context.Context, operation, query string, args ...any) (bool, error) {
	request, cancel := operationContext(ctx)
	defer cancel()
	if err := requestError(request); err != nil {
		return false, fmt.Errorf("anylock/postgres: %s: %w", operation, err)
	}
	result, err := d.db.ExecContext(request, query, args...)
	if operationErr := requestError(request); err != nil || operationErr != nil {
		return false, fmt.Errorf("anylock/postgres: %s: %w", operation, errors.Join(err, operationErr))
	}
	if result == nil {
		return false, fmt.Errorf("anylock/postgres: %s: nil result", operation)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("anylock/postgres: %s: rows affected: %w", operation, err)
	}
	if count < 0 || count > 1 {
		return false, fmt.Errorf("anylock/postgres: %s: unexpected rows affected: %d", operation, count)
	}
	return count == 1, nil
}

func (d *Driver) validateContext(ctx context.Context) error {
	if d == nil || d.db == nil {
		return errors.New("anylock/postgres: nil driver or database")
	}
	if ctx == nil {
		return errors.New("anylock/postgres: nil context")
	}
	return requestError(ctx)
}

func (d *Driver) validate(ctx context.Context, key, owner string) error {
	if err := d.validateContext(ctx); err != nil {
		return err
	}
	if key == "" || owner == "" || !validText(key) || !validText(owner) {
		return errors.New("anylock/postgres: key and owner must be nonempty UTF-8 without NUL")
	}
	return nil
}

func operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, time.Second)
}

func requestError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func validText(value string) bool { return utf8.ValidString(value) && !strings.ContainsRune(value, 0) }

func quoteTable(name string) (string, error) {
	parts := strings.Split(name, ".")
	if len(parts) == 1 {
		parts = []string{"public", parts[0]}
	}
	if len(parts) != 2 {
		return "", errors.New("anylock/postgres: table must be table or schema.table")
	}
	for _, part := range parts {
		if !validIdentifier(part) {
			return "", errors.New("anylock/postgres: invalid table identifier")
		}
	}
	if parts[0] == "pg_temp" || strings.HasPrefix(parts[0], "pg_temp_") {
		return "", errors.New("anylock/postgres: temporary schemas are not shared lease storage")
	}
	return `"` + parts[0] + `"."` + parts[1] + `"`, nil
}

func validIdentifier(name string) bool {
	if name == "" || len(name) > 63 {
		return false
	}
	for i := range len(name) {
		c := name[i]
		letter := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_'
		digit := i > 0 && c >= '0' && c <= '9'
		if !letter && !digit {
			return false
		}
	}
	return true
}

func ttlInterval(ttl time.Duration) (string, error) {
	if ttl <= 0 {
		return "", errors.New("anylock/postgres: TTL must be positive")
	}
	microseconds := int64(ttl / time.Microsecond)
	if ttl%time.Microsecond != 0 {
		microseconds++
	}
	// PostgreSQL 12 interval text fields are int32. Split even extreme durations
	// into small integer units, without floating point or calendar-day/DST math.
	hours := microseconds / 3_600_000_000
	minutes := microseconds / 60_000_000 % 60
	seconds := microseconds / 1_000_000 % 60
	return fmt.Sprintf("%d hours %d minutes %d seconds %d microseconds",
		hours, minutes, seconds, microseconds%1_000_000), nil
}

// WithFenceSequence selects the permanent sequence used for fenced acquisitions.
// Names follow WithTable rules. It must be ascending, NO CYCLE, CACHE 1, and must
// never be reset/replaced while any token can remain in downstream storage.
func WithFenceSequence(name string) Option {
	return func(d *Driver) error {
		sequence, err := quoteTable(name)
		if err != nil {
			return err
		}
		d.sequence = sequence
		return nil
	}
}

// EnsureFencingSchema explicitly provisions the fencing sequence in addition to
// EnsureSchema's lease table. Existing objects are not migrated or validated.
// Provision through a serialized production migration with CACHE 1, NO CYCLE.
func (d *Driver) EnsureFencingSchema(ctx context.Context) error {
	if err := d.EnsureSchema(ctx); err != nil {
		return err
	}
	query := "CREATE SEQUENCE IF NOT EXISTS " + d.sequence + " AS bigint NO CYCLE CACHE 1"
	_, err := d.exec(ctx, "fencing schema", query)
	return err
}

// acquireFenced allocates nextval only AFTER acquisition, while the modifying
// CTE still holds the lease row lock. Allocating in INSERT's VALUES before an
// ON CONFLICT wait could grant an older token to a newer owner. Sequence gaps
// (including aborted operations) are harmless. Missing sequence is an error.
func (d *Driver) acquireFenced(
	ctx context.Context, key, owner, interval string,
) (anylock.AcquireResult, error) {
	query := "WITH acquired AS (" + fmt.Sprintf(acquireSQL, d.table) +
		" RETURNING key) SELECT pg_catalog.nextval($4::pg_catalog.regclass) FROM acquired"
	request, cancel := operationContext(ctx)
	defer cancel()
	if contextErr := requestError(request); contextErr != nil {
		return anylock.AcquireResult{}, contextErr
	}
	var token int64
	err := d.db.QueryRowContext(request, query, d.prefix+key, owner, interval, d.sequence).Scan(&token)
	if requestErr := requestError(request); requestErr != nil {
		return anylock.AcquireResult{}, errors.Join(err, requestErr)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return anylock.AcquireResult{}, nil
	}
	if err != nil {
		return anylock.AcquireResult{}, fmt.Errorf("anylock/postgres: fenced acquire: %w", err)
	}
	if token <= 0 {
		return anylock.AcquireResult{}, errors.New("anylock/postgres: invalid fencing token")
	}
	return anylock.AcquireResult{Acquired: true, Token: uint64(token)}, nil
}

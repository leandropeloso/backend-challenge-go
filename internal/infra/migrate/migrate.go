// Package migrate aplica e reverte as migrations SQL embutidas.
package migrate

import (
	"context"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// advisoryLockID impede que duas instâncias migrem ao mesmo tempo.
const advisoryLockID = 727274

var fileName = regexp.MustCompile(`^(\d+)_([a-z0-9_]+)\.(up|down)\.sql$`)

type migration struct {
	version int64
	name    string
	up      string
	down    string
}

type Runner struct {
	conn  *pgx.Conn
	files []migration
}

func Load(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, err
	}
	byVersion := map[int64]*migration{}
	for _, e := range entries {
		m := fileName.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		v, _ := strconv.ParseInt(m[1], 10, 64)
		body, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, err
		}
		mig := byVersion[v]
		if mig == nil {
			mig = &migration{version: v, name: m[2]}
			byVersion[v] = mig
		}
		if m[3] == "up" {
			mig.up = string(body)
		} else {
			mig.down = string(body)
		}
	}
	out := make([]migration, 0, len(byVersion))
	for _, m := range byVersion {
		if m.up == "" || m.down == "" {
			return nil, fmt.Errorf("migration %04d_%s needs both .up.sql and .down.sql", m.version, m.name)
		}
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

func New(ctx context.Context, databaseURL string, fsys fs.FS) (*Runner, error) {
	files, err := Load(fsys)
	if err != nil {
		return nil, err
	}
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	r := &Runner{conn: conn, files: files}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryLockID); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("acquire migration lock: %w", err)
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version bigint PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("create schema_migrations: %w", err)
	}
	return r, nil
}

func (r *Runner) Close(ctx context.Context) error {
	_, _ = r.conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryLockID)
	return r.conn.Close(ctx)
}

func (r *Runner) applied(ctx context.Context) (map[int64]bool, error) {
	rows, err := r.conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	set := map[int64]bool{}
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		set[v] = true
	}
	return set, rows.Err()
}

// Up aplica todas as migrations pendentes, cada uma em sua transação.
func (r *Runner) Up(ctx context.Context) (int, error) {
	done, err := r.applied(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, m := range r.files {
		if done[m.version] {
			continue
		}
		if err := r.run(ctx, m.up, `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, m.version, m.name); err != nil {
			return n, fmt.Errorf("apply %04d_%s: %w", m.version, m.name, err)
		}
		n++
	}
	return n, nil
}

// Down reverte as últimas steps migrations aplicadas (0 = todas).
func (r *Runner) Down(ctx context.Context, steps int) (int, error) {
	done, err := r.applied(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for i := len(r.files) - 1; i >= 0; i-- {
		m := r.files[i]
		if !done[m.version] {
			continue
		}
		if steps > 0 && n >= steps {
			break
		}
		if err := r.run(ctx, m.down, `DELETE FROM schema_migrations WHERE version = $1`, m.version); err != nil {
			return n, fmt.Errorf("revert %04d_%s: %w", m.version, m.name, err)
		}
		n++
	}
	return n, nil
}

type Status struct {
	Version int64
	Name    string
	Applied bool
}

func (r *Runner) Status(ctx context.Context) ([]Status, error) {
	done, err := r.applied(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Status, 0, len(r.files))
	for _, m := range r.files {
		out = append(out, Status{Version: m.version, Name: m.name, Applied: done[m.version]})
	}
	return out, nil
}

func (r *Runner) run(ctx context.Context, script, bookkeeping string, args ...any) error {
	tx, err := r.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, script); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, bookkeeping, args...); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

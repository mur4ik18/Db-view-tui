package db

import (
	"context"
	"dbctl/internal/config"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type Options struct {
	ConnectionName string
	URL            string
	Timeout        time.Duration
	Schema         string
	Verbose        bool
}

type Session struct {
	Conn   *pgx.Conn
	Label  string
	Schema string
}

func Open(ctx context.Context, cfg *config.Config, opts Options) (*Session, error) {
	label, connConfig, err := Resolve(ctx, cfg, opts)
	if err != nil {
		return nil, err
	}

	connCtx := ctx
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		connCtx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	conn, err := pgx.ConnectConfig(connCtx, connConfig)
	if err != nil {
		return nil, err
	}

	session := &Session{
		Conn:   conn,
		Label:  label,
		Schema: opts.Schema,
	}

	if opts.Schema != "" {
		if _, err := conn.Exec(ctx, fmt.Sprintf("SET search_path TO %s", QuoteIdentifier(opts.Schema))); err != nil {
			_ = conn.Close(ctx)
			return nil, fmt.Errorf("set schema: %w", err)
		}
	}

	return session, nil
}

func Resolve(_ context.Context, cfg *config.Config, opts Options) (string, *pgx.ConnConfig, error) {
	if opts.URL != "" {
		parsed, err := pgx.ParseConfig(opts.URL)
		if err != nil {
			return "", nil, fmt.Errorf("parse url: %w", err)
		}
		return "adhoc", parsed, nil
	}

	if cfg == nil {
		return "", nil, fmt.Errorf("config is required")
	}

	name := opts.ConnectionName
	if name == "" {
		name = cfg.Default
	}
	if name == "" {
		return "", nil, fmt.Errorf("no connection selected; use -c or dbctl connect use")
	}

	conn, ok := cfg.Connections[name]
	if !ok {
		return "", nil, fmt.Errorf("connection %q not found", name)
	}

	if conn.URL != "" {
		parsed, err := pgx.ParseConfig(conn.URL)
		if err != nil {
			return "", nil, fmt.Errorf("parse saved url: %w", err)
		}
		if password := config.EnvPassword(name); password != "" {
			parsed.Password = password
		}
		return name, parsed, nil
	}

	if conn.Host == "" || conn.Database == "" || conn.User == "" {
		return "", nil, fmt.Errorf("connection %q is incomplete", name)
	}

	if conn.Port == 0 {
		conn.Port = 5432
	}
	if conn.SSLMode == "" {
		conn.SSLMode = "require"
	}

	dsn := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s sslmode=%s",
		conn.Host,
		conn.Port,
		conn.Database,
		conn.User,
		conn.SSLMode,
	)

	password := config.EnvPassword(name)
	if password == "" {
		password = conn.Password
	}
	if password != "" {
		dsn += " password=" + quoteValue(password)
	}

	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		return "", nil, fmt.Errorf("parse connection: %w", err)
	}

	return name, parsed, nil
}

func Query(ctx context.Context, conn *pgx.Conn, sql string, args ...any) ([]string, [][]string, int64, error) {
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, 0, err
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	columns := make([]string, 0, len(fields))
	for _, field := range fields {
		columns = append(columns, field.Name)
	}

	data := make([][]string, 0)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, nil, 0, err
		}

		record := make([]string, 0, len(values))
		for _, value := range values {
			record = append(record, FormatValue(value))
		}
		data = append(data, record)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, 0, err
	}

	return columns, data, rows.CommandTag().RowsAffected(), nil
}

func QueryRunner[T interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}](ctx context.Context, runner T, sql string, args ...any) ([]string, [][]string, int64, error) {
	rows, err := runner.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, 0, err
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	columns := make([]string, 0, len(fields))
	for _, field := range fields {
		columns = append(columns, field.Name)
	}

	data := make([][]string, 0)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, nil, 0, err
		}

		record := make([]string, 0, len(values))
		for _, value := range values {
			record = append(record, FormatValue(value))
		}
		data = append(data, record)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, 0, err
	}

	return columns, data, rows.CommandTag().RowsAffected(), nil
}

func FormatValue(value any) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case []byte:
		return string(v)
	case time.Time:
		return v.Format(time.RFC3339)
	default:
		return fmt.Sprint(v)
	}
}

func QuoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func BuildPreviewSQL(schema string, table string, limit int) string {
	if limit <= 0 {
		limit = 50
	}
	if schema == "" {
		schema = "public"
	}

	return fmt.Sprintf(
		"SELECT * FROM %s.%s LIMIT %d;",
		QuoteIdentifier(schema),
		QuoteIdentifier(table),
		limit,
	)
}

func quoteValue(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `\'`) + `'`
}

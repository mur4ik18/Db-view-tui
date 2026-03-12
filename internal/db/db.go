package db

import (
	"context"
	"database/sql/driver"
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

type PreviewFilter struct {
	Column   string
	Include  string
	Excludes []string
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
	case driver.Valuer:
		text, err := v.Value()
		if err == nil {
			return FormatValue(text)
		}
		return fmt.Sprint(v)
	default:
		return fmt.Sprint(v)
	}
}

func QuoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func buildFilteredFromClause(schema string, table string, filters []PreviewFilter) (string, []any) {
	if schema == "" {
		schema = "public"
	}

	var builder strings.Builder
	args := make([]any, 0, 8)
	builder.WriteString(" FROM ")
	builder.WriteString(QuoteIdentifier(schema))
	builder.WriteString(".")
	builder.WriteString(QuoteIdentifier(table))

	hasWhere := false
	for _, filter := range filters {
		column := strings.TrimSpace(filter.Column)
		include := strings.TrimSpace(filter.Include)
		if column == "" || (include == "" && len(filter.Excludes) == 0) {
			continue
		}
		if hasWhere {
			builder.WriteString(" AND ")
		} else {
			builder.WriteString(" WHERE ")
			hasWhere = true
		}
		builder.WriteString("(")

		wroteClause := false
		if include != "" {
			args = append(args, "%"+include+"%")
			builder.WriteString("CAST(")
			builder.WriteString(QuoteIdentifier(column))
			builder.WriteString(" AS text) ILIKE $")
			builder.WriteString(fmt.Sprint(len(args)))
			wroteClause = true
		}

		for _, excludeValue := range filter.Excludes {
			excludeValue = strings.TrimSpace(excludeValue)
			if excludeValue == "" {
				continue
			}
			if wroteClause {
				builder.WriteString(" AND ")
			}
			args = append(args, "%"+excludeValue+"%")
			builder.WriteString("CAST(")
			builder.WriteString(QuoteIdentifier(column))
			builder.WriteString(" AS text) NOT ILIKE $")
			builder.WriteString(fmt.Sprint(len(args)))
			wroteClause = true
		}

		if !wroteClause {
			builder.WriteString("TRUE")
		}
		builder.WriteString(")")
	}

	return builder.String(), args
}

func BuildPreviewQuery(schema string, table string, limit int, offset int, sortColumn string, sortDesc bool, distinctColumn string, filters []PreviewFilter) (string, []any) {
	if limit <= 0 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	fromClause, args := buildFilteredFromClause(schema, table, filters)
	var builder strings.Builder
	distinctColumn = strings.TrimSpace(distinctColumn)
	sortColumn = strings.TrimSpace(sortColumn)

	if distinctColumn != "" {
		builder.WriteString("SELECT * FROM (SELECT DISTINCT ON (")
		builder.WriteString(QuoteIdentifier(distinctColumn))
		builder.WriteString(") *")
		builder.WriteString(fromClause)
		builder.WriteString(" ORDER BY ")
		builder.WriteString(QuoteIdentifier(distinctColumn))
		builder.WriteString(" ASC")
		if sortColumn != "" && !strings.EqualFold(sortColumn, distinctColumn) {
			builder.WriteString(", ")
			builder.WriteString(QuoteIdentifier(sortColumn))
			if sortDesc {
				builder.WriteString(" DESC")
			} else {
				builder.WriteString(" ASC")
			}
		}
		builder.WriteString(") AS preview")
		if sortColumn != "" {
			builder.WriteString(" ORDER BY ")
			builder.WriteString(QuoteIdentifier(sortColumn))
			if sortDesc {
				builder.WriteString(" DESC")
			} else {
				builder.WriteString(" ASC")
			}
		} else {
			builder.WriteString(" ORDER BY ")
			builder.WriteString(QuoteIdentifier(distinctColumn))
			builder.WriteString(" ASC")
		}
	} else {
		builder.WriteString("SELECT *")
		builder.WriteString(fromClause)
		if sortColumn != "" {
			builder.WriteString(" ORDER BY ")
			builder.WriteString(QuoteIdentifier(sortColumn))
			if sortDesc {
				builder.WriteString(" DESC")
			} else {
				builder.WriteString(" ASC")
			}
		}
	}
	builder.WriteString(" OFFSET ")
	builder.WriteString(fmt.Sprint(offset))
	builder.WriteString(" LIMIT ")
	builder.WriteString(fmt.Sprint(limit))
	builder.WriteString(";")
	return builder.String(), args
}

func BuildCountQuery(schema string, table string, distinctColumn string, filters []PreviewFilter) (string, []any) {
	fromClause, args := buildFilteredFromClause(schema, table, filters)
	distinctColumn = strings.TrimSpace(distinctColumn)
	if distinctColumn != "" {
		var builder strings.Builder
		builder.WriteString("SELECT count(*) FROM (SELECT DISTINCT ON (")
		builder.WriteString(QuoteIdentifier(distinctColumn))
		builder.WriteString(") 1")
		builder.WriteString(fromClause)
		builder.WriteString(" ORDER BY ")
		builder.WriteString(QuoteIdentifier(distinctColumn))
		builder.WriteString(" ASC) AS counted;")
		return builder.String(), args
	}
	return "SELECT count(*)" + fromClause + ";", args
}

func BuildPreviewSQL(schema string, table string, limit int, offset int) string {
	sql, _ := BuildPreviewQuery(schema, table, limit, offset, "", false, "", nil)
	return sql
}

func quoteValue(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `\'`) + `'`
}

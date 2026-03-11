package sqlrun

import (
	"context"
	"dbctl/internal/db"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
)

type Statement struct {
	File      string
	Text      string
	StartLine int
}

type Result struct {
	File      string
	Statement string
	Status    string
	Rows      string
	Time      string
	Error     string
}

type ExecuteOptions struct {
	Transaction bool
	StopOnError bool
	Pattern     string
}

type Summary struct {
	Results  []Result
	Elapsed  time.Duration
	Failures int
}

func Collect(path string, pattern string, vars map[string]string) ([]Statement, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if pattern == "" {
		pattern = "*.sql"
	}

	if info.IsDir() {
		matches, err := filepath.Glob(filepath.Join(path, pattern))
		if err != nil {
			return nil, err
		}
		sort.Strings(matches)

		var statements []Statement
		for _, match := range matches {
			parsed, err := readStatements(match, vars)
			if err != nil {
				return nil, err
			}
			statements = append(statements, parsed...)
		}
		return statements, nil
	}

	return readStatements(path, vars)
}

func Execute(ctx context.Context, conn *pgx.Conn, statements []Statement, opts ExecuteOptions) (Summary, error) {
	start := time.Now()
	results := make([]Result, 0, len(statements))
	failures := 0

	if opts.StopOnError || !opts.Transaction && !opts.StopOnError {
		// no-op; keeps behavior explicit for callers
	}

	if opts.Transaction {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return Summary{}, err
		}

		for _, stmt := range statements {
			result, err := executeOne(ctx, tx, stmt)
			results = append(results, result)
			if err != nil {
				failures++
				if opts.StopOnError {
					_ = tx.Rollback(ctx)
					return Summary{Results: results, Elapsed: time.Since(start), Failures: failures}, fmt.Errorf("%s:%d: %w", stmt.File, stmt.StartLine, err)
				}
			}
		}

		if failures > 0 {
			_ = tx.Rollback(ctx)
			return Summary{Results: results, Elapsed: time.Since(start), Failures: failures}, nil
		}

		if err := tx.Commit(ctx); err != nil {
			return Summary{Results: results, Elapsed: time.Since(start), Failures: failures}, err
		}

		return Summary{Results: results, Elapsed: time.Since(start), Failures: failures}, nil
	}

	for _, stmt := range statements {
		result, err := executeOne(ctx, conn, stmt)
		results = append(results, result)
		if err != nil {
			failures++
			if opts.StopOnError {
				return Summary{Results: results, Elapsed: time.Since(start), Failures: failures}, fmt.Errorf("%s:%d: %w", stmt.File, stmt.StartLine, err)
			}
		}
	}

	return Summary{Results: results, Elapsed: time.Since(start), Failures: failures}, nil
}

func ParseVars(values []string) map[string]string {
	out := make(map[string]string, len(values))
	for _, value := range values {
		key, raw, ok := strings.Cut(value, "=")
		if !ok {
			continue
		}
		out[strings.TrimSpace(key)] = strings.TrimSpace(raw)
	}
	return out
}

func PreviewSQL(sql string) string {
	sql = strings.Join(strings.Fields(sql), " ")
	if len(sql) <= 60 {
		return sql
	}
	return sql[:57] + "..."
}

func ApplyVars(sql string, vars map[string]string) string {
	for key, value := range vars {
		sql = strings.ReplaceAll(sql, "{{"+key+"}}", value)
	}
	return sql
}

func SplitSQL(input string) []Statement {
	var (
		statements      []Statement
		current         strings.Builder
		line            = 1
		startLine       = 1
		hasContent      bool
		inSingleQuote   bool
		inDoubleQuote   bool
		inLineComment   bool
		inBlockComment  bool
		dollarDelimiter string
	)

	runes := []rune(input)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		next := rune(0)
		if i+1 < len(runes) {
			next = runes[i+1]
		}

		if !hasContent && !unicode.IsSpace(r) {
			startLine = line
			hasContent = true
		}

		if dollarDelimiter != "" {
			if hasDelimiterAt(runes, i, dollarDelimiter) {
				current.WriteString(dollarDelimiter)
				i += len([]rune(dollarDelimiter)) - 1
				dollarDelimiter = ""
				continue
			}
			current.WriteRune(r)
			if r == '\n' {
				line++
			}
			continue
		}

		if inLineComment {
			current.WriteRune(r)
			if r == '\n' {
				line++
				inLineComment = false
			}
			continue
		}

		if inBlockComment {
			current.WriteRune(r)
			if r == '\n' {
				line++
			}
			if r == '*' && next == '/' {
				current.WriteRune(next)
				i++
				inBlockComment = false
			}
			continue
		}

		if inSingleQuote {
			current.WriteRune(r)
			if r == '\n' {
				line++
			}
			if r == '\'' && next == '\'' {
				current.WriteRune(next)
				i++
				continue
			}
			if r == '\'' {
				inSingleQuote = false
			}
			continue
		}

		if inDoubleQuote {
			current.WriteRune(r)
			if r == '\n' {
				line++
			}
			if r == '"' && next == '"' {
				current.WriteRune(next)
				i++
				continue
			}
			if r == '"' {
				inDoubleQuote = false
			}
			continue
		}

		if r == '-' && next == '-' {
			current.WriteRune(r)
			current.WriteRune(next)
			i++
			inLineComment = true
			continue
		}

		if r == '/' && next == '*' {
			current.WriteRune(r)
			current.WriteRune(next)
			i++
			inBlockComment = true
			continue
		}

		if delimiter, ok := readDollarDelimiter(runes, i); ok {
			current.WriteString(delimiter)
			i += len([]rune(delimiter)) - 1
			dollarDelimiter = delimiter
			continue
		}

		if r == '\'' {
			current.WriteRune(r)
			inSingleQuote = true
			continue
		}

		if r == '"' {
			current.WriteRune(r)
			inDoubleQuote = true
			continue
		}

		current.WriteRune(r)
		if r == '\n' {
			line++
		}

		if r == ';' {
			text := strings.TrimSpace(current.String())
			if text != "" && text != ";" {
				statements = append(statements, Statement{
					Text:      text,
					StartLine: startLine,
				})
			}
			current.Reset()
			hasContent = false
			startLine = line
		}
	}

	text := strings.TrimSpace(current.String())
	if text != "" {
		statements = append(statements, Statement{
			Text:      text,
			StartLine: startLine,
		})
	}

	return statements
}

func readStatements(path string, vars map[string]string) ([]Statement, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	text := ApplyVars(string(data), vars)
	raw := SplitSQL(text)
	statements := make([]Statement, 0, len(raw))
	for _, stmt := range raw {
		statements = append(statements, Statement{
			File:      filepath.Base(path),
			Text:      stmt.Text,
			StartLine: stmt.StartLine,
		})
	}
	return statements, nil
}

func executeOne[T interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}](ctx context.Context, runner T, stmt Statement) (Result, error) {
	begin := time.Now()
	_, _, affected, err := db.QueryRunner(ctx, runner, stmt.Text)
	result := Result{
		File:      stmt.File,
		Statement: PreviewSQL(stmt.Text),
		Status:    "OK",
		Rows:      "-",
		Time:      time.Since(begin).Round(time.Millisecond).String(),
	}

	if affected > 0 {
		result.Rows = fmt.Sprint(affected)
	}

	if err != nil {
		result.Status = "FAIL"
		result.Error = err.Error()
		return result, err
	}

	return result, nil
}

func readDollarDelimiter(runes []rune, index int) (string, bool) {
	if runes[index] != '$' {
		return "", false
	}

	j := index + 1
	for j < len(runes) && runes[j] != '$' {
		if !(unicode.IsLetter(runes[j]) || unicode.IsDigit(runes[j]) || runes[j] == '_') {
			return "", false
		}
		j++
	}

	if j >= len(runes) || runes[j] != '$' {
		return "", false
	}

	return string(runes[index : j+1]), true
}

func hasDelimiterAt(runes []rune, index int, delimiter string) bool {
	delim := []rune(delimiter)
	if index+len(delim) > len(runes) {
		return false
	}
	for offset, r := range delim {
		if runes[index+offset] != r {
			return false
		}
	}
	return true
}

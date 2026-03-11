package repl

import (
	"context"
	"dbctl/internal/config"
	"dbctl/internal/db"
	"dbctl/internal/output"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/chzyer/readline"
	"github.com/jackc/pgx/v5"
)

type Options struct {
	Conn          *pgx.Conn
	Label         string
	Schema        string
	InitialFormat output.Format
	Timeout       time.Duration
}

func Run(ctx context.Context, opts Options) error {
	if err := config.EnsureDir(); err != nil {
		return err
	}

	rl, err := readline.NewEx(&readline.Config{
		Prompt:          prompt(false),
		HistoryFile:     config.HistoryPath(),
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		return err
	}
	defer rl.Close()

	output.Infof("connected to %s", opts.Label)

	var (
		buffer     strings.Builder
		multiline  bool
		timing     bool
		format     = opts.InitialFormat
		nextExport string
	)

	for {
		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			if buffer.Len() == 0 {
				continue
			}
			buffer.Reset()
			multiline = false
			rl.SetPrompt(prompt(false))
			continue
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		trimmed := strings.TrimSpace(line)
		if !multiline && strings.HasPrefix(trimmed, `\`) {
			done, err := handleMetaCommand(ctx, opts, trimmed, &format, &timing, &nextExport)
			if err != nil {
				output.Errorf("%v", err)
			}
			if done {
				return nil
			}
			continue
		}

		if trimmed == "" && !multiline {
			continue
		}

		buffer.WriteString(line)
		buffer.WriteByte('\n')

		statement := strings.TrimSpace(buffer.String())
		if !strings.HasSuffix(statement, ";") {
			multiline = true
			rl.SetPrompt(prompt(true))
			continue
		}

		multiline = false
		rl.SetPrompt(prompt(false))

		runCtx := ctx
		if opts.Timeout > 0 {
			var cancel context.CancelFunc
			runCtx, cancel = context.WithTimeout(ctx, opts.Timeout)
			defer cancel()
		}

		start := time.Now()
		columns, rows, affected, err := db.Query(runCtx, opts.Conn, statement)
		elapsed := time.Since(start)
		buffer.Reset()

		if err != nil {
			output.Errorf("%v", err)
			continue
		}

		if nextExport != "" {
			if err := output.RenderToFile(nextExport, format, columns, rows); err != nil {
				output.Errorf("export failed: %v", err)
			} else {
				output.Successf("exported result to %s", nextExport)
			}
			nextExport = ""
		}

		if err := output.Render(format, columns, rows, os.Stdout); err != nil {
			return err
		}

		if timing {
			if affected > 0 {
				output.Mutedf("%d rows affected in %s", affected, elapsed.Round(time.Millisecond))
			} else {
				output.Mutedf("completed in %s", elapsed.Round(time.Millisecond))
			}
		}
	}
}

func handleMetaCommand(
	ctx context.Context,
	opts Options,
	command string,
	format *output.Format,
	timing *bool,
	nextExport *string,
) (bool, error) {
	parts := strings.Fields(command)
	switch parts[0] {
	case `\q`:
		return true, nil
	case `\clear`:
		fmt.Print("\033[H\033[2J")
		return false, nil
	case `\timing`:
		*timing = !*timing
		output.Infof("timing %t", *timing)
		return false, nil
	case `\format`:
		if len(parts) != 2 {
			return false, fmt.Errorf(`usage: \format json|csv|table|yaml`)
		}
		value, err := output.ParseFormat(parts[1])
		if err != nil {
			return false, err
		}
		*format = value
		output.Successf("output format set to %s", value)
		return false, nil
	case `\export`:
		if len(parts) != 2 {
			return false, fmt.Errorf(`usage: \export <file>`)
		}
		*nextExport = parts[1]
		output.Infof("next result will be saved to %s", *nextExport)
		return false, nil
	case `\refresh`:
		output.Successf("metadata cache refreshed")
		return false, nil
	case `\dt`:
		schema := opts.Schema
		if len(parts) == 2 {
			schema = parts[1]
		}
		return false, runBuiltin(ctx, opts.Conn, *format, db.TablesSQL(true), schema)
	case `\d`:
		if len(parts) != 2 {
			return false, fmt.Errorf(`usage: \d <table>`)
		}
		return false, runBuiltin(ctx, opts.Conn, *format, db.DescribeTableSQL(), parts[1])
	case `\du`:
		return false, runBuiltin(ctx, opts.Conn, *format, db.UsersSQL())
	case `\di`:
		if len(parts) == 2 {
			return false, runBuiltin(ctx, opts.Conn, *format, db.IndexesSQL(true), parts[1])
		}
		return false, runBuiltin(ctx, opts.Conn, *format, db.IndexesSQL(false))
	case `\ds`:
		return false, runBuiltin(ctx, opts.Conn, *format, db.SizesSQL(), 20)
	case `\da`:
		return false, runBuiltin(ctx, opts.Conn, *format, db.ActivitySQL())
	case `\dl`:
		return false, runBuiltin(ctx, opts.Conn, *format, db.LocksSQL())
	default:
		return false, fmt.Errorf("unknown meta-command: %s", parts[0])
	}
}

func runBuiltin(ctx context.Context, conn *pgx.Conn, format output.Format, sql string, args ...any) error {
	columns, rows, _, err := db.Query(ctx, conn, sql, args...)
	if err != nil {
		return err
	}
	return output.Render(format, columns, rows, os.Stdout)
}

func prompt(multiline bool) string {
	if multiline {
		return "   ...> "
	}
	return "dbctl> "
}

package cmd

import (
	"context"
	"dbctl/internal/config"
	"dbctl/internal/db"
	"dbctl/internal/output"
	"dbctl/internal/tui"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var (
	globalConnection string
	globalURL        string
	globalFormat     string
	globalNoColor    bool
	globalVerbose    bool
	globalTimeout    time.Duration
	globalSchema     string
)

var rootCmd = &cobra.Command{
	Use:           "dbctl",
	Short:         "CLI for remote PostgreSQL work",
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(_ *cobra.Command, _ []string) error {
		return tui.Run(tui.Options{
			Schema:  globalSchema,
			Timeout: globalTimeout,
			URL:     globalURL,
		})
	},
	PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
		output.SetColor(!globalNoColor)
		_, err := output.ParseFormat(globalFormat)
		return err
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		output.Errorf("%v", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&globalConnection, "connection", "c", "", "saved connection name")
	rootCmd.PersistentFlags().StringVar(&globalURL, "url", "", "adhoc postgres url")
	rootCmd.PersistentFlags().StringVarP(&globalFormat, "format", "f", "table", "output format: table|json|csv|yaml")
	rootCmd.PersistentFlags().BoolVar(&globalNoColor, "no-color", false, "disable color output")
	rootCmd.PersistentFlags().BoolVarP(&globalVerbose, "verbose", "v", false, "verbose logging")
	rootCmd.PersistentFlags().DurationVar(&globalTimeout, "timeout", 30*time.Second, "query timeout")
	rootCmd.PersistentFlags().StringVar(&globalSchema, "schema", "public", "default schema")
}

func loadConfig() (*config.Config, error) {
	return config.Load()
}

func currentFormat() output.Format {
	format, err := output.ParseFormat(globalFormat)
	if err != nil {
		return output.FormatTable
	}
	return format
}

func openSession(ctx context.Context, overrideName string) (*db.Session, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}

	name := globalConnection
	if overrideName != "" {
		name = overrideName
	}

	return db.Open(ctx, cfg, db.Options{
		ConnectionName: name,
		URL:            globalURL,
		Timeout:        globalTimeout,
		Schema:         globalSchema,
		Verbose:        globalVerbose,
	})
}

func renderResult(columns []string, rows [][]string) error {
	return output.Render(currentFormat(), columns, rows, os.Stdout)
}

func runQuery(ctx context.Context, session *db.Session, sql string, args ...any) ([]string, [][]string, int64, time.Duration, error) {
	queryCtx := ctx
	if globalTimeout > 0 {
		var cancel context.CancelFunc
		queryCtx, cancel = context.WithTimeout(ctx, globalTimeout)
		defer cancel()
	}

	start := time.Now()
	columns, rows, affected, err := db.Query(queryCtx, session.Conn, sql, args...)
	return columns, rows, affected, time.Since(start), err
}

func summarizeExecution(affected int64, elapsed time.Duration) {
	if affected > 0 {
		output.Mutedf("%d rows affected in %s", affected, elapsed.Round(time.Millisecond))
		return
	}
	output.Mutedf("completed in %s", elapsed.Round(time.Millisecond))
}

func requireArg(args []string, index int, name string) (string, error) {
	if len(args) <= index || args[index] == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return args[index], nil
}

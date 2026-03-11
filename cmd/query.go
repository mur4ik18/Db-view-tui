package cmd

import (
	"context"
	"strings"

	"github.com/spf13/cobra"
)

func init() {
	queryCmd := &cobra.Command{
		Use:   "query <sql>",
		Short: "Run a one-off SQL query",
		Args:  cobra.MinimumNArgs(1),
		RunE:  runQueryCommand,
	}

	rootCmd.AddCommand(queryCmd)
}

func runQueryCommand(_ *cobra.Command, args []string) error {
	ctx := context.Background()
	session, err := openSession(ctx, "")
	if err != nil {
		return err
	}
	defer session.Conn.Close(ctx)

	sql := strings.Join(args, " ")
	columns, rows, affected, elapsed, err := runQuery(ctx, session, sql)
	if err != nil {
		return err
	}

	if err := renderResult(columns, rows); err != nil {
		return err
	}

	summarizeExecution(affected, elapsed)
	return nil
}

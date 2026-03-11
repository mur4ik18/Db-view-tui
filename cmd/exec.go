package cmd

import (
	"context"
	"dbctl/internal/sqlrun"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var (
	execTransaction     bool
	execDryRun          bool
	execStopOnError     bool
	execContinueOnError bool
	execPattern         string
	execVars            []string
)

func init() {
	execCmd := &cobra.Command{
		Use:   "exec <file-or-dir>",
		Short: "Execute SQL files",
		Args:  cobra.ExactArgs(1),
		RunE:  runExecCommand,
	}

	execCmd.Flags().BoolVar(&execTransaction, "transaction", false, "run inside a transaction")
	execCmd.Flags().BoolVar(&execDryRun, "dry-run", false, "parse and show statements without executing")
	execCmd.Flags().BoolVar(&execStopOnError, "stop-on-error", true, "stop on first error")
	execCmd.Flags().BoolVar(&execContinueOnError, "continue-on-error", false, "continue after errors")
	execCmd.Flags().StringVar(&execPattern, "pattern", "*.sql", "glob for directory execution")
	execCmd.Flags().StringArrayVar(&execVars, "var", nil, "template variable replacement: key=value")

	rootCmd.AddCommand(execCmd)
}

func runExecCommand(_ *cobra.Command, args []string) error {
	if execContinueOnError {
		execStopOnError = false
	}

	statements, err := sqlrun.Collect(args[0], execPattern, sqlrun.ParseVars(execVars))
	if err != nil {
		return err
	}
	if len(statements) == 0 {
		return fmt.Errorf("no SQL statements found")
	}

	if execDryRun {
		rows := make([][]string, 0, len(statements))
		for idx, stmt := range statements {
			rows = append(rows, []string{
				fmt.Sprint(idx + 1),
				stmt.File,
				fmt.Sprint(stmt.StartLine),
				sqlrun.PreviewSQL(stmt.Text),
			})
		}
		return renderResult([]string{"#", "file", "line", "statement"}, rows)
	}

	ctx := context.Background()
	session, err := openSession(ctx, "")
	if err != nil {
		return err
	}
	defer session.Conn.Close(ctx)

	summary, err := sqlrun.Execute(ctx, session.Conn, statements, sqlrun.ExecuteOptions{
		Transaction: execTransaction,
		StopOnError: execStopOnError,
		Pattern:     execPattern,
	})
	if err != nil {
		return err
	}

	if err := renderResult([]string{"#", "file", "statement", "status", "rows", "time"}, addOrdinal(summary.Results)); err != nil {
		return err
	}

	if summary.Failures > 0 {
		return fmt.Errorf("%d statement(s) failed after %s", summary.Failures, summary.Elapsed.Round(time.Millisecond))
	}

	outputSuccessSummary(len(statements), summary.Elapsed, execTransaction)
	return nil
}

func addOrdinal(rows []sqlrun.Result) [][]string {
	out := make([][]string, 0, len(rows))
	for i, row := range rows {
		out = append(out, []string{
			fmt.Sprint(i + 1),
			row.File,
			row.Statement,
			row.Status,
			row.Rows,
			row.Time,
		})
	}
	return out
}

func outputSuccessSummary(count int, elapsed time.Duration, inTx bool) {
	if inTx {
		outputText := fmt.Sprintf("executed %d statements successfully in %s, COMMIT", count, elapsed.Round(time.Millisecond))
		fmt.Fprintln(os.Stderr, outputText)
		return
	}
	fmt.Fprintf(os.Stderr, "executed %d statements successfully in %s\n", count, elapsed.Round(time.Millisecond))
}

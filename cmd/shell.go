package cmd

import (
	"context"
	"dbctl/internal/repl"

	"github.com/spf13/cobra"
)

func init() {
	shellCmd := &cobra.Command{
		Use:   "shell [connection_name]",
		Short: "Start interactive SQL shell",
		Args:  cobra.MaximumNArgs(1),
		RunE:  runShellCommand,
	}

	rootCmd.AddCommand(shellCmd)
}

func runShellCommand(_ *cobra.Command, args []string) error {
	override := ""
	if len(args) > 0 {
		override = args[0]
	}

	ctx := context.Background()
	session, err := openSession(ctx, override)
	if err != nil {
		return err
	}
	defer session.Conn.Close(ctx)

	return repl.Run(ctx, repl.Options{
		Conn:          session.Conn,
		Label:         session.Label,
		Schema:        session.Schema,
		InitialFormat: currentFormat(),
		Timeout:       globalTimeout,
	})
}

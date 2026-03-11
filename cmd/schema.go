package cmd

import (
	"context"
	"dbctl/internal/db"
	"fmt"

	"github.com/spf13/cobra"
)

var (
	schemaTablesFilter string
	indexesTable       string
	sizesTop           int
	dataLimit          int
)

func init() {
	schemaCmd := &cobra.Command{
		Use:   "schema",
		Short: "Inspect database schema",
	}

	tablesCmd := &cobra.Command{
		Use:   "tables",
		Short: "List tables",
		RunE: func(_ *cobra.Command, _ []string) error {
			if schemaTablesFilter != "" {
				return runSchemaSQL(db.TablesSQL(true), schemaTablesFilter)
			}
			return runSchemaSQL(db.TablesSQL(false))
		},
	}
	tablesCmd.Flags().StringVar(&schemaTablesFilter, "schema", "", "filter schema")

	describeCmd := &cobra.Command{
		Use:   "describe <table>",
		Short: "Describe table columns",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runSchemaSQL(db.DescribeTableSQL(), args[0])
		},
	}

	columnsCmd := &cobra.Command{
		Use:   "columns <table>",
		Short: "List columns with constraints",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runSchemaSQL(db.ColumnConstraintsSQL(), args[0])
		},
	}

	privilegesCmd := &cobra.Command{
		Use:   "privileges <table>",
		Short: "Show table privileges",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runSchemaSQL(db.TablePrivilegesSQL(), args[0])
		},
	}

	dataCmd := &cobra.Command{
		Use:   "data <table>",
		Short: "Preview table data",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runSchemaDynamic(func() string {
				return db.BuildPreviewSQL(globalSchema, args[0], dataLimit)
			})
		},
	}
	dataCmd.Flags().IntVar(&dataLimit, "limit", 50, "preview row limit")

	usersCmd := &cobra.Command{
		Use:   "users",
		Short: "List roles and users",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runSchemaSQL(db.UsersSQL())
		},
	}

	indexesCmd := &cobra.Command{
		Use:   "indexes",
		Short: "List indexes",
		RunE: func(_ *cobra.Command, _ []string) error {
			if indexesTable != "" {
				return runSchemaSQL(db.IndexesSQL(true), indexesTable)
			}
			return runSchemaSQL(db.IndexesSQL(false))
		},
	}
	indexesCmd.Flags().StringVar(&indexesTable, "table", "", "filter table")

	sizesCmd := &cobra.Command{
		Use:   "sizes",
		Short: "Show table sizes",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runSchemaSQL(db.SizesSQL(), sizesTop)
		},
	}
	sizesCmd.Flags().IntVar(&sizesTop, "top", 20, "top N tables")

	fkeysCmd := &cobra.Command{
		Use:   "fkeys <table>",
		Short: "List foreign keys for a table",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runSchemaSQL(db.FKeysSQL(), args[0])
		},
	}

	enumsCmd := &cobra.Command{
		Use:   "enums",
		Short: "List enum types",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runSchemaSQL(db.EnumsSQL())
		},
	}

	activityCmd := &cobra.Command{
		Use:   "activity",
		Short: "Show active sessions",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runSchemaSQL(db.ActivitySQL())
		},
	}

	locksCmd := &cobra.Command{
		Use:   "locks",
		Short: "Show lock waits",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runSchemaSQL(db.LocksSQL())
		},
	}

	schemaCmd.AddCommand(
		tablesCmd,
		describeCmd,
		columnsCmd,
		privilegesCmd,
		dataCmd,
		usersCmd,
		indexesCmd,
		sizesCmd,
		fkeysCmd,
		enumsCmd,
		activityCmd,
		locksCmd,
	)

	rootCmd.AddCommand(schemaCmd)
}

func runSchemaSQL(sql string, args ...any) error {
	ctx := context.Background()
	session, err := openSession(ctx, "")
	if err != nil {
		return err
	}
	defer session.Conn.Close(ctx)

	columns, rows, _, elapsed, err := runQuery(ctx, session, sql, args...)
	if err != nil {
		return err
	}

	if len(columns) == 0 {
		return fmt.Errorf("query returned no columns")
	}

	if err := renderResult(columns, rows); err != nil {
		return err
	}

	summarizeExecution(int64(len(rows)), elapsed)
	return nil
}

func runSchemaDynamic(sql func() string) error {
	ctx := context.Background()
	session, err := openSession(ctx, "")
	if err != nil {
		return err
	}
	defer session.Conn.Close(ctx)

	columns, rows, _, elapsed, err := runQuery(ctx, session, sql())
	if err != nil {
		return err
	}

	if err := renderResult(columns, rows); err != nil {
		return err
	}

	summarizeExecution(int64(len(rows)), elapsed)
	return nil
}

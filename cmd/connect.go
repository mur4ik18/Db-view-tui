package cmd

import (
	"bufio"
	"context"
	"dbctl/internal/config"
	"dbctl/internal/db"
	"dbctl/internal/output"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var (
	addHost     string
	addPort     int
	addDatabase string
	addUser     string
	addPassword string
	addSSLMode  string
	addURL      string
)

func init() {
	connectCmd := &cobra.Command{
		Use:   "connect",
		Short: "Manage saved connections",
	}

	addCmd := &cobra.Command{
		Use:   "add <name>",
		Short: "Add or update a saved connection",
		Args:  cobra.ExactArgs(1),
		RunE:  runConnectAdd,
	}
	addCmd.Flags().StringVar(&addHost, "host", "", "database host")
	addCmd.Flags().IntVar(&addPort, "port", 5432, "database port")
	addCmd.Flags().StringVar(&addDatabase, "db", "", "database name")
	addCmd.Flags().StringVar(&addUser, "user", "", "database user")
	addCmd.Flags().StringVar(&addPassword, "password", "", "database password")
	addCmd.Flags().StringVar(&addSSLMode, "sslmode", "require", "postgres sslmode")
	addCmd.Flags().StringVar(&addURL, "url", "", "postgres connection url")

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List saved connections",
		RunE:  runConnectList,
	}

	rmCmd := &cobra.Command{
		Use:   "rm <name>",
		Short: "Remove a saved connection",
		Args:  cobra.ExactArgs(1),
		RunE:  runConnectRemove,
	}

	useCmd := &cobra.Command{
		Use:   "use <name>",
		Short: "Set default connection",
		Args:  cobra.ExactArgs(1),
		RunE:  runConnectUse,
	}

	testCmd := &cobra.Command{
		Use:   "test [name]",
		Short: "Test a connection",
		Args:  cobra.MaximumNArgs(1),
		RunE:  runConnectTest,
	}

	connectCmd.AddCommand(addCmd, listCmd, rmCmd, useCmd, testCmd)
	rootCmd.AddCommand(connectCmd)
}

func runConnectAdd(_ *cobra.Command, args []string) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	name := args[0]
	conn := config.Connection{
		Host:     addHost,
		Port:     addPort,
		Database: addDatabase,
		User:     addUser,
		Password: addPassword,
		SSLMode:  addSSLMode,
		URL:      addURL,
	}

	if conn.URL == "" && (conn.Host == "" || conn.Database == "" || conn.User == "") {
		conn, err = promptConnection(conn)
		if err != nil {
			return err
		}
	}

	cfg.Connections[name] = conn
	if cfg.Default == "" {
		cfg.Default = name
	}

	if err := config.Save(cfg); err != nil {
		return err
	}

	output.Successf("saved connection %q", name)
	return nil
}

func runConnectList(_ *cobra.Command, _ []string) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	rows := make([][]string, 0, len(cfg.Connections))
	for name, conn := range cfg.Connections {
		marker := ""
		if name == cfg.Default {
			marker = "*"
		}

		host := conn.Host
		database := conn.Database
		user := conn.User
		source := "fields"
		if conn.URL != "" {
			host = "(from url)"
			database = "-"
			user = "-"
			source = "url"
		}
		rows = append(rows, []string{marker, name, host, database, user, conn.SSLMode, source})
	}

	if len(rows) == 0 {
		output.Mutedf("no saved connections yet")
		return nil
	}

	return renderResult(
		[]string{"default", "name", "host", "database", "user", "sslmode", "source"},
		rows,
	)
}

func runConnectRemove(_ *cobra.Command, args []string) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	name := args[0]
	if _, ok := cfg.Connections[name]; !ok {
		return fmt.Errorf("connection %q not found", name)
	}

	delete(cfg.Connections, name)
	if cfg.Default == name {
		cfg.Default = ""
		for candidate := range cfg.Connections {
			cfg.Default = candidate
			break
		}
	}

	if err := config.Save(cfg); err != nil {
		return err
	}

	output.Successf("removed connection %q", name)
	return nil
}

func runConnectUse(_ *cobra.Command, args []string) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	name := args[0]
	if _, ok := cfg.Connections[name]; !ok {
		return fmt.Errorf("connection %q not found", name)
	}

	cfg.Default = name
	if err := config.Save(cfg); err != nil {
		return err
	}

	output.Successf("default connection set to %q", name)
	return nil
}

func runConnectTest(_ *cobra.Command, args []string) error {
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

	columns, rows, _, elapsed, err := runQuery(ctx, session, db.ConnectionTestSQL())
	if err != nil {
		return err
	}

	if err := renderResult(columns, rows); err != nil {
		return err
	}

	output.Successf("connected to %q in %s", session.Label, elapsed.Round(0))
	return nil
}

func promptConnection(current config.Connection) (config.Connection, error) {
	reader := bufio.NewReader(os.Stdin)

	if current.URL == "" {
		url := promptText(reader, "Postgres URL (leave empty to use host fields)", "")
		if url != "" {
			current.URL = url
			return current, nil
		}
	}

	current.Host = promptText(reader, "Host", current.Host)
	portText := promptText(reader, "Port", strconv.Itoa(defaultPort(current.Port)))
	port, err := strconv.Atoi(portText)
	if err != nil {
		return current, fmt.Errorf("invalid port: %w", err)
	}
	current.Port = port
	current.Database = promptText(reader, "Database", current.Database)
	current.User = promptText(reader, "User", current.User)
	current.SSLMode = promptText(reader, "SSL mode", defaultString(current.SSLMode, "require"))

	if current.Password == "" {
		fmt.Fprint(os.Stdout, "Password: ")
		password, err := term.ReadPassword(int(syscall.Stdin))
		fmt.Fprintln(os.Stdout)
		if err != nil {
			return current, err
		}
		current.Password = strings.TrimSpace(string(password))
	}

	return current, nil
}

func promptText(reader *bufio.Reader, label string, fallback string) string {
	if fallback != "" {
		fmt.Fprintf(os.Stdout, "%s [%s]: ", label, fallback)
	} else {
		fmt.Fprintf(os.Stdout, "%s: ", label)
	}

	value, _ := reader.ReadString('\n')
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func defaultString(value string, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func defaultPort(value int) int {
	if value != 0 {
		return value
	}
	return 5432
}

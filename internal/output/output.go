package output

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/fatih/color"
	"gopkg.in/yaml.v3"
)

type Format string

const (
	FormatTable Format = "table"
	FormatJSON  Format = "json"
	FormatCSV   Format = "csv"
	FormatYAML  Format = "yaml"
)

var (
	success = color.New(color.FgGreen, color.Bold)
	failure = color.New(color.FgRed, color.Bold)
	info    = color.New(color.FgCyan, color.Bold)
	muted   = color.New(color.FgHiBlack)
)

func SetColor(enabled bool) {
	color.NoColor = !enabled
}

func ParseFormat(value string) (Format, error) {
	switch Format(strings.ToLower(strings.TrimSpace(value))) {
	case "", FormatTable:
		return FormatTable, nil
	case FormatJSON:
		return FormatJSON, nil
	case FormatCSV:
		return FormatCSV, nil
	case FormatYAML:
		return FormatYAML, nil
	default:
		return "", fmt.Errorf("unsupported format %q", value)
	}
}

func Render(format Format, columns []string, rows [][]string, w io.Writer) error {
	switch format {
	case FormatJSON:
		return renderJSON(columns, rows, w)
	case FormatCSV:
		return renderCSV(columns, rows, w)
	case FormatYAML:
		return renderYAML(columns, rows, w)
	default:
		return renderTable(columns, rows, w)
	}
}

func RenderToFile(path string, format Format, columns []string, rows [][]string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return Render(format, columns, rows, f)
}

func Successf(format string, args ...any) {
	_, _ = success.Fprintf(os.Stderr, "OK  "+format+"\n", args...)
}

func Errorf(format string, args ...any) {
	_, _ = failure.Fprintf(os.Stderr, "ERR "+format+"\n", args...)
}

func Infof(format string, args ...any) {
	_, _ = info.Fprintf(os.Stderr, ">> "+format+"\n", args...)
}

func Mutedf(format string, args ...any) {
	_, _ = muted.Fprintf(os.Stderr, format+"\n", args...)
}

func renderJSON(columns []string, rows [][]string, w io.Writer) error {
	records := rowsToMaps(columns, rows)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(records)
}

func renderCSV(columns []string, rows [][]string, w io.Writer) error {
	writer := csv.NewWriter(w)
	if err := writer.Write(columns); err != nil {
		return err
	}
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func renderYAML(columns []string, rows [][]string, w io.Writer) error {
	data, err := yaml.Marshal(rowsToMaps(columns, rows))
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func renderTable(columns []string, rows [][]string, w io.Writer) error {
	if len(columns) == 0 {
		return nil
	}

	widths := make([]int, len(columns))
	alignRight := make([]bool, len(columns))

	for i, col := range columns {
		widths[i] = len(col)
	}

	for col := range columns {
		alignRight[col] = true
		for _, row := range rows {
			if col >= len(row) {
				continue
			}
			cell := row[col]
			if len(cell) > widths[col] {
				widths[col] = len(cell)
			}
			if !looksNumeric(cell) {
				alignRight[col] = false
			}
		}
	}

	border := buildBorder(widths)
	if _, err := fmt.Fprintln(w, border); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, buildRow(columns, widths, make([]bool, len(columns)))); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, border); err != nil {
		return err
	}

	for _, row := range rows {
		if _, err := fmt.Fprintln(w, buildRow(row, widths, alignRight)); err != nil {
			return err
		}
	}

	_, err := fmt.Fprintln(w, border)
	return err
}

func buildBorder(widths []int) string {
	var b strings.Builder
	b.WriteString("+")
	for _, width := range widths {
		b.WriteString(strings.Repeat("-", width+2))
		b.WriteString("+")
	}
	return b.String()
}

func buildRow(values []string, widths []int, alignRight []bool) string {
	var b strings.Builder
	b.WriteString("|")
	for i, width := range widths {
		value := ""
		if i < len(values) {
			value = values[i]
		}
		if alignRight[i] {
			value = padLeft(value, width)
		} else {
			value = padRight(value, width)
		}
		b.WriteString(" ")
		b.WriteString(value)
		b.WriteString(" |")
	}
	return b.String()
}

func padLeft(value string, width int) string {
	if len(value) >= width {
		return value
	}
	return strings.Repeat(" ", width-len(value)) + value
}

func padRight(value string, width int) string {
	if len(value) >= width {
		return value
	}
	return value + strings.Repeat(" ", width-len(value))
}

func looksNumeric(value string) bool {
	if value == "" || strings.EqualFold(value, "null") {
		return false
	}
	_, err := strconv.ParseFloat(strings.ReplaceAll(value, ",", ""), 64)
	return err == nil
}

func rowsToMaps(columns []string, rows [][]string) []map[string]string {
	records := make([]map[string]string, 0, len(rows))
	for _, row := range rows {
		record := make(map[string]string, len(columns))
		for i, col := range columns {
			if i < len(row) {
				record[col] = row[i]
				continue
			}
			record[col] = ""
		}
		records = append(records, record)
	}
	return records
}

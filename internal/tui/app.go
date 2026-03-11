package tui

import (
	"bytes"
	"context"
	"dbctl/internal/config"
	"dbctl/internal/db"
	"dbctl/internal/output"
	"dbctl/internal/sqlrun"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type Options struct {
	Schema  string
	Timeout time.Duration
	URL     string
}

type tab int

const (
	tabConnections tab = iota
	tabSchema
	tabQuery
	tabExec
)

type connTestMsg struct {
	name string
	text string
	err  error
}

type schemaMsg struct {
	title    string
	text     string
	err      error
	columns  []string
	rows     [][]string
	dataMode bool
}

type queryMsg struct {
	text string
	err  error
}

type execMsg struct {
	text string
	err  error
}

type formSaveMsg struct {
	cfg     *config.Config
	current string
	err     error
}

type deleteConnMsg struct {
	cfg     *config.Config
	current string
	err     error
}

type setDefaultMsg struct {
	cfg *config.Config
	err error
}

type schemaTablesMsg struct {
	tables []string
	err    error
}

type dataPageMsg struct {
	title    string
	schema   string
	table    string
	columns  []string
	rows     [][]string
	offset   int
	pageSize int
	err      error
}

type dataPresetSaveMsg struct {
	cfg  *config.Config
	name string
	err  error
}

type dataPresetDeleteMsg struct {
	cfg  *config.Config
	name string
	err  error
}

type model struct {
	opts Options

	width  int
	height int

	activeTab     tab
	status        string
	busy          bool
	currentSchema string

	cfg               *config.Config
	connectionNames   []string
	connectionIndex   int
	currentConnection string
	selectedTable     string

	schemaActions []string
	schemaIndex   int
	schemaArg     textinput.Model
	schemaArgEdit bool

	queryEditor textarea.Model
	queryResult viewport.Model

	execPath          textinput.Model
	execPattern       textinput.Model
	execVars          textarea.Model
	execResult        viewport.Model
	execFocus         int
	execDryRun        bool
	execTransaction   bool
	execContinueOnErr bool

	detail viewport.Model

	formOpen       bool
	formEditingOld string
	formFields     []textinput.Model
	formFieldIndex int

	schemaSwitchOpen  bool
	schemaSwitchInput textinput.Model

	schemaTablePickerOpen bool
	schemaTableFilter     textinput.Model
	schemaTableNames      []string
	schemaTableIndex      int
	schemaTableFocus      bool

	dataColumns     []string
	dataRows        [][]string
	dataRowOffset   int
	dataColOffset   int
	dataFocus       bool
	dataPinnedCol   string
	dataSelectedRow int
	dataSelectedCol int
	dataPageSize    int
	dataLoadingMore bool
	dataEOF         bool
	dataSortCol     string
	dataSortDesc    bool
	dataFilters     []db.PreviewFilter
	dataInspect     viewport.Model
	dataInspectOpen bool

	dataPresetSaveOpen bool
	dataPresetListOpen bool
	dataPresetName     textinput.Model
	dataPresetIndex    int
}

func Run(opts Options) error {
	m := newModel(opts)
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}

func newModel(opts Options) model {
	schemaArg := textinput.New()
	schemaArg.Placeholder = "limit search -exclude"
	schemaArg.CharLimit = 256
	schemaArg.Width = 24

	queryEditor := textarea.New()
	queryEditor.Placeholder = "SELECT * FROM your_table LIMIT 20;"
	queryEditor.SetWidth(80)
	queryEditor.SetHeight(12)
	queryEditor.Focus()

	execPath := textinput.New()
	execPath.Placeholder = "path to .sql file or directory"
	execPath.CharLimit = 512
	execPath.Width = 64

	execPattern := textinput.New()
	execPattern.SetValue("*.sql")
	execPattern.Placeholder = "*.sql"
	execPattern.CharLimit = 128
	execPattern.Width = 32

	execVars := textarea.New()
	execVars.Placeholder = "key=value\nother=123"
	execVars.SetHeight(5)
	execVars.SetWidth(40)

	schemaSwitchInput := textinput.New()
	schemaSwitchInput.Placeholder = "public"
	schemaSwitchInput.CharLimit = 256
	schemaSwitchInput.Width = 32

	schemaTableFilter := textinput.New()
	schemaTableFilter.Placeholder = "filter tables"
	schemaTableFilter.CharLimit = 256
	schemaTableFilter.Width = 40

	dataPresetName := textinput.New()
	dataPresetName.Placeholder = "preset name"
	dataPresetName.CharLimit = 128
	dataPresetName.Width = 40

	m := model{
		opts: opts,
		cfg: &config.Config{
			Connections: map[string]config.Connection{},
		},
		status:            "Ready",
		currentSchema:     defaultString(opts.Schema, "public"),
		schemaActions:     []string{"Tables", "Indexes", "Users", "Sizes", "Enums", "Activity", "Locks"},
		schemaArg:         schemaArg,
		queryEditor:       queryEditor,
		queryResult:       viewport.New(0, 0),
		execPath:          execPath,
		execPattern:       execPattern,
		execVars:          execVars,
		execResult:        viewport.New(0, 0),
		detail:            viewport.New(0, 0),
		dataInspect:       viewport.New(0, 0),
		dataPresetName:    dataPresetName,
		execFocus:         0,
		execDryRun:        false,
		execTransaction:   false,
		schemaSwitchInput: schemaSwitchInput,
		schemaTableFilter: schemaTableFilter,
	}

	if cfg, err := config.Load(); err == nil {
		m.cfg = cfg
		m.refreshConnectionNames()
	}
	m.focusExec()
	return m
}

func (m model) Init() tea.Cmd {
	return nil
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.resize()
		return m, nil
	case tea.KeyMsg:
		if m.formOpen {
			return m.handleFormKeys(msg)
		}
		if m.schemaSwitchOpen {
			return m.handleSchemaSwitchKeys(msg)
		}
		if m.dataInspectOpen {
			return m.handleDataInspectKeys(msg)
		}
		if m.dataPresetSaveOpen {
			return m.handleDataPresetSaveKeys(msg)
		}
		if m.dataPresetListOpen {
			return m.handleDataPresetListKeys(msg)
		}
		if m.activeTab == tabSchema && m.schemaArgEdit {
			return m.handleSchemaKeys(msg)
		}
		switch msg.String() {
		case "ctrl+c", "ctrl+d":
			return m, tea.Quit
		case "s":
			m.schemaSwitchOpen = true
			m.schemaSwitchInput.SetValue(m.currentSchema)
			m.schemaSwitchInput.CursorEnd()
			m.schemaSwitchInput.Focus()
			m.status = "Switch schema"
			return m, nil
		case "1":
			m.activeTab = tabConnections
			m.status = "Connections"
			return m, nil
		case "2":
			m.activeTab = tabSchema
			m.status = "Schema"
			return m, nil
		case "3":
			m.activeTab = tabQuery
			m.status = "Query editor"
			return m, nil
		case "4":
			m.activeTab = tabExec
			m.status = "SQL executor"
			return m, nil
		}

		switch m.activeTab {
		case tabConnections:
			return m.handleConnectionsKeys(msg)
		case tabSchema:
			return m.handleSchemaKeys(msg)
		case tabQuery:
			return m.handleQueryKeys(msg)
		case tabExec:
			return m.handleExecKeys(msg)
		}
	case connTestMsg:
		m.busy = false
		if msg.err != nil {
			m.status = msg.err.Error()
			return m, nil
		}
		m.status = "Connection test finished"
		m.detail.SetContent(msg.text)
		return m, nil
	case schemaMsg:
		m.busy = false
		if msg.err != nil {
			m.status = msg.err.Error()
			return m, nil
		}
		m.status = msg.title
		m.dataFocus = false
		m.detail.SetContent(msg.text)
		return m, nil
	case dataPageMsg:
		m.busy = false
		m.dataLoadingMore = false
		if msg.err != nil {
			m.status = msg.err.Error()
			return m, nil
		}
		if msg.schema != m.effectiveSchema() || msg.table != m.selectedTable {
			return m, nil
		}
		m.schemaIndex = m.indexOfSchemaAction("Data")
		m.dataPageSize = msg.pageSize
		m.dataEOF = len(msg.rows) < msg.pageSize
		if msg.offset == 0 {
			selectedColumn := m.selectedDataColumnName()
			m.dataColumns = append([]string(nil), msg.columns...)
			m.dataRows = append([][]string(nil), msg.rows...)
			m.dataRowOffset = 0
			m.dataColOffset = 0
			m.dataSelectedRow = 0
			m.dataSelectedCol = indexOfStringFold(msg.columns, selectedColumn)
			if m.dataSelectedCol < 0 {
				m.dataSelectedCol = 0
			}
		} else {
			m.dataRows = append(m.dataRows, msg.rows...)
		}
		m.normalizeDataState()
		m.dataFocus = true
		m.detail.SetContent("")
		m.status = msg.title
		if msg.offset == 0 {
			m.status += " | arrows move, f pin, esc back"
		}
		return m, nil
	case dataPresetSaveMsg:
		m.busy = false
		if msg.err != nil {
			m.status = msg.err.Error()
			return m, nil
		}
		m.cfg = msg.cfg
		m.dataPresetSaveOpen = false
		m.dataPresetName.Blur()
		m.status = "Saved preset: " + msg.name
		return m, nil
	case dataPresetDeleteMsg:
		m.busy = false
		if msg.err != nil {
			m.status = msg.err.Error()
			return m, nil
		}
		m.cfg = msg.cfg
		presets := m.dataPresetsForSelectedTable()
		if len(presets) == 0 {
			m.dataPresetListOpen = false
			m.dataPresetIndex = 0
		} else if m.dataPresetIndex >= len(presets) {
			m.dataPresetIndex = len(presets) - 1
		}
		m.status = "Deleted preset: " + msg.name
		return m, nil
	case queryMsg:
		m.busy = false
		if msg.err != nil {
			m.status = msg.err.Error()
			return m, nil
		}
		m.status = "Query executed"
		m.queryResult.SetContent(msg.text)
		return m, nil
	case execMsg:
		m.busy = false
		if msg.err != nil {
			m.status = msg.err.Error()
			return m, nil
		}
		m.status = "Execution finished"
		m.execResult.SetContent(msg.text)
		return m, nil
	case formSaveMsg:
		m.busy = false
		if msg.err != nil {
			m.status = msg.err.Error()
			return m, nil
		}
		m.cfg = msg.cfg
		m.refreshConnectionNames()
		m.currentConnection = msg.current
		m.formOpen = false
		m.status = "Connection saved"
		return m, nil
	case deleteConnMsg:
		m.busy = false
		if msg.err != nil {
			m.status = msg.err.Error()
			return m, nil
		}
		m.cfg = msg.cfg
		m.refreshConnectionNames()
		m.currentConnection = msg.current
		m.status = "Connection removed"
		return m, nil
	case setDefaultMsg:
		m.busy = false
		if msg.err != nil {
			m.status = msg.err.Error()
			return m, nil
		}
		m.cfg = msg.cfg
		m.refreshConnectionNames()
		m.status = "Default connection updated"
		return m, nil
	case schemaTablesMsg:
		m.busy = false
		if msg.err != nil {
			m.status = msg.err.Error()
			return m, nil
		}
		m.schemaTableNames = msg.tables
		m.schemaTableIndex = 0
		m.schemaTableFocus = true
		m.status = "Choose table in the right panel"
		return m, nil
	}

	var cmd tea.Cmd
	switch m.activeTab {
	case tabQuery:
		m.queryEditor, cmd = m.queryEditor.Update(msg)
	case tabExec:
		switch m.execFocus {
		case 0:
			m.execPath, cmd = m.execPath.Update(msg)
		case 1:
			m.execPattern, cmd = m.execPattern.Update(msg)
		case 2:
			m.execVars, cmd = m.execVars.Update(msg)
		}
	}
	return m, cmd
}

func (m model) View() string {
	if m.width == 0 || m.height == 0 {
		return "loading..."
	}

	header := m.viewHeader()
	body := m.viewBody()
	if m.formOpen {
		body = m.viewFormScreen()
	}
	if m.schemaSwitchOpen {
		body = m.viewSchemaSwitchScreen()
	}
	if m.dataInspectOpen {
		body = m.viewDataInspectScreen()
	}
	if m.dataPresetSaveOpen {
		body = m.viewDataPresetSaveScreen()
	}
	if m.dataPresetListOpen {
		body = m.viewDataPresetListScreen()
	}
	footer := m.viewFooter()

	content := lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
	return content
}

func (m *model) handleConnectionsKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "up", "k":
		if m.connectionIndex > 0 {
			m.connectionIndex--
		}
	case "down", "j":
		if m.connectionIndex < len(m.connectionNames)-1 {
			m.connectionIndex++
		}
	case "enter":
		if name := m.selectedConnectionName(); name != "" {
			m.currentConnection = name
			m.status = fmt.Sprintf("Current connection: %s", name)
		}
	case "u":
		name := m.selectedConnectionName()
		if name == "" {
			m.status = "No connection selected"
			return m, nil
		}
		m.busy = true
		return m, setDefaultCmd(name)
	case "t":
		name := m.selectedConnectionName()
		if name == "" {
			m.status = "No connection selected"
			return m, nil
		}
		m.busy = true
		return m, testConnectionCmd(m.opts, name)
	case "n":
		m.openForm("", config.Connection{})
	case "e":
		name := m.selectedConnectionName()
		if name == "" {
			m.status = "No connection selected"
			return m, nil
		}
		m.openForm(name, m.cfg.Connections[name])
	case "d":
		name := m.selectedConnectionName()
		if name == "" {
			m.status = "No connection selected"
			return m, nil
		}
		m.busy = true
		return m, deleteConnectionCmd(name)
	}
	return m, nil
}

func (m *model) handleSchemaKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.schemaArgEdit {
		switch msg.String() {
		case "esc":
			m.schemaArgEdit = false
			m.schemaArg.Blur()
			m.status = "Schema input closed"
			return m, nil
		case "enter":
			m.schemaArgEdit = false
			m.schemaArg.Blur()
			return m.runSchemaAction()
		}

		var cmd tea.Cmd
		m.schemaArg, cmd = m.schemaArg.Update(msg)
		return m, cmd
	}

	if m.dataFocus && m.showingDataPreview() {
		switch msg.String() {
		case "up", "k":
			m.dataFocus = true
			if m.dataSelectedRow > 0 {
				m.dataSelectedRow--
			}
			m.ensureDataSelectionVisible()
			m.status = m.dataStatus()
			return m, nil
		case "down", "j":
			m.dataFocus = true
			if m.dataSelectedRow < len(m.dataRows)-1 {
				m.dataSelectedRow++
			}
			m.ensureDataSelectionVisible()
			if cmd := m.maybeLoadMoreData(); cmd != nil {
				m.status = "Loading more rows..."
				return m, cmd
			}
			m.status = m.dataStatus()
			return m, nil
		case "pgup":
			m.dataFocus = true
			m.dataSelectedRow = max(0, m.dataSelectedRow-m.dataRowCapacity())
			m.ensureDataSelectionVisible()
			m.status = m.dataStatus()
			return m, nil
		case "pgdown":
			m.dataFocus = true
			m.dataSelectedRow = min(max(0, len(m.dataRows)-1), m.dataSelectedRow+m.dataRowCapacity())
			m.ensureDataSelectionVisible()
			if cmd := m.maybeLoadMoreData(); cmd != nil {
				m.status = "Loading more rows..."
				return m, cmd
			}
			m.status = m.dataStatus()
			return m, nil
		case "left", "h":
			m.dataFocus = true
			if m.dataSelectedCol > 0 {
				m.dataSelectedCol--
			}
			m.ensureDataSelectionVisible()
			m.status = m.dataStatus()
			return m, nil
		case "right", "l":
			m.dataFocus = true
			if m.dataSelectedCol < len(m.dataColumns)-1 {
				m.dataSelectedCol++
			}
			m.ensureDataSelectionVisible()
			m.status = m.dataStatus()
			return m, nil
		case "home", "g":
			m.dataFocus = true
			m.dataSelectedRow = 0
			m.dataSelectedCol = 0
			m.ensureDataSelectionVisible()
			m.status = m.dataStatus()
			return m, nil
		case "end", "G":
			m.dataFocus = true
			if len(m.dataRows) > 0 {
				m.dataSelectedRow = len(m.dataRows) - 1
			}
			m.ensureDataSelectionVisible()
			if cmd := m.maybeLoadMoreData(); cmd != nil {
				m.status = "Loading more rows..."
				return m, cmd
			}
			m.status = m.dataStatus()
			return m, nil
		case "tab", "esc", "q":
			if m.dataFocus {
				m.dataFocus = false
				m.status = "Schema actions"
				return m, nil
			}
		}
	}

	if m.schemaTableFocus && m.isTableListAction() {
		switch msg.String() {
		case "up", "k":
			if m.schemaTableIndex > 0 {
				m.schemaTableIndex--
			}
			return m, nil
		case "down", "j":
			if m.schemaTableIndex < len(m.schemaTableNames)-1 {
				m.schemaTableIndex++
			}
			return m, nil
		case "enter":
			if len(m.schemaTableNames) == 0 {
				m.status = "No tables found"
				return m, nil
			}
			schema, table := splitQualifiedTable(m.schemaTableNames[m.schemaTableIndex])
			m.currentSchema = defaultString(schema, m.currentSchema)
			m.selectedTable = table
			m.clearDataPreview()
			m.schemaTableFocus = false
			m.status = "Columns " + schema + "." + table
			m.schemaIndex = m.indexOfSchemaAction("Columns")
			m.busy = true
			return m, runSchemaCmd(m.opts, m.effectiveConnection(), m.effectiveSchema(), "Columns "+m.selectedTable, db.ColumnConstraintsSQL(), m.selectedTable)
		case "tab", "left", "h", "esc", "q":
			m.schemaTableFocus = false
			m.status = "Schema actions"
			return m, nil
		}
	}

	switch msg.String() {
	case "up", "k":
		if m.schemaIndex > 0 {
			m.schemaIndex--
		}
	case "down", "j":
		if m.schemaIndex < len(m.visibleSchemaActions())-1 {
			m.schemaIndex++
		}
	case "a", "/":
		if m.showingDataPreview() {
			m.schemaArg.SetValue(m.selectedColumnFilterInput())
			m.schemaArg.CursorEnd()
		}
		m.schemaArgEdit = true
		m.schemaArg.Focus()
		m.status = "Edit schema argument"
	case "tab", "right", "l":
		if m.isTableListAction() && len(m.schemaTableNames) > 0 {
			m.schemaTableFocus = true
			m.status = "Choose table in the right panel"
		} else if m.showingDataPreview() {
			m.dataFocus = true
			m.status = "Data grid focus"
		}
	case "x":
		if m.selectedTable != "" {
			m.selectedTable = ""
			m.clearDataPreview()
			m.schemaIndex = 0
			m.status = "Table study cleared"
		}
	case "f":
		if !m.showingDataPreview() {
			break
		}
		if len(m.dataColumns) == 0 {
			m.status = "Load data first"
			return m, nil
		}
		column := m.dataColumns[m.dataSelectedCol]
		if strings.EqualFold(m.dataPinnedCol, column) {
			m.dataPinnedCol = ""
			m.status = "Pinned column cleared"
			return m, nil
		}
		m.dataPinnedCol = column
		m.ensureDataSelectionVisible()
		m.status = "Pinned column: " + column
		return m, nil
	case "o":
		if !m.showingDataPreview() || len(m.dataColumns) == 0 {
			break
		}
		column := m.dataColumns[m.dataSelectedCol]
		if strings.EqualFold(m.dataSortCol, column) {
			m.dataSortDesc = !m.dataSortDesc
		} else {
			m.dataSortCol = column
			m.dataSortDesc = false
		}
		m.busy = true
		return m, runDataPreviewCmd(
			m.opts,
			m.effectiveConnection(),
			m.effectiveSchema(),
			m.selectedTable,
			max(100, m.dataPageSize),
			m.dataSortCol,
			m.dataSortDesc,
			m.dataFilters,
		)
	case "r":
		if !m.showingDataPreview() {
			break
		}
		m.schemaArg.SetValue("")
		m.dataSortCol = ""
		m.dataSortDesc = false
		m.dataFilters = nil
		m.busy = true
		return m, runDataPreviewCmd(
			m.opts,
			m.effectiveConnection(),
			m.effectiveSchema(),
			m.selectedTable,
			max(100, m.dataPageSize),
			"",
			false,
			nil,
		)
	case "d":
		if !m.showingDataPreview() {
			break
		}
		column := m.selectedDataColumnName()
		if column == "" {
			m.status = "No column selected"
			return m, nil
		}
		if !m.removeDataFilter(column) {
			m.status = "No filter on column: " + column
			return m, nil
		}
		m.schemaArg.SetValue("")
		m.busy = true
		return m, runDataPreviewCmd(
			m.opts,
			m.effectiveConnection(),
			m.effectiveSchema(),
			m.selectedTable,
			max(100, m.dataPageSize),
			m.dataSortCol,
			m.dataSortDesc,
			m.dataFilters,
		)
	case "w":
		if !m.showingDataPreview() {
			break
		}
		if m.currentDataTableKey() == "" {
			m.status = "Choose table first"
			return m, nil
		}
		m.dataPresetSaveOpen = true
		m.dataPresetName.SetValue("")
		m.dataPresetName.Focus()
		m.status = "Save current data preset"
		return m, nil
	case "p":
		if !m.showingDataPreview() {
			break
		}
		m.dataPresetListOpen = true
		m.dataPresetIndex = 0
		m.status = "Choose preset"
		return m, nil
	case "v":
		if !m.showingDataPreview() || len(m.dataColumns) == 0 || len(m.dataRows) == 0 {
			m.status = "Load data first"
			return m, nil
		}
		m.openDataInspect()
		return m, nil
	case "enter":
		if m.showingDataPreview() {
			m.dataFocus = true
			m.status = m.dataStatus()
			return m, nil
		}
		return m.runSchemaAction()
	}

	return m, nil
}

func (m *model) handleQueryKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+e":
		sql := strings.TrimSpace(m.queryEditor.Value())
		if sql == "" {
			m.status = "Query is empty"
			return m, nil
		}
		m.busy = true
		return m, runQueryCmd(m.opts, m.effectiveConnection(), m.effectiveSchema(), sql)
	case "ctrl+k":
		m.queryEditor.SetValue("")
		m.queryResult.SetContent("")
		m.status = "Query cleared"
		return m, nil
	}
	return m, nil
}

func (m *model) handleExecKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "tab":
		m.execFocus = (m.execFocus + 1) % 3
		m.focusExec()
		return m, nil
	case "shift+tab":
		m.execFocus--
		if m.execFocus < 0 {
			m.execFocus = 2
		}
		m.focusExec()
		return m, nil
	case "ctrl+r":
		m.execDryRun = !m.execDryRun
		return m, nil
	case "ctrl+t":
		m.execTransaction = !m.execTransaction
		return m, nil
	case "ctrl+g":
		m.execContinueOnErr = !m.execContinueOnErr
		return m, nil
	case "ctrl+e":
		path := strings.TrimSpace(m.execPath.Value())
		if path == "" {
			m.status = "Path is required"
			return m, nil
		}
		m.busy = true
		return m, runExecCmd(
			m.opts,
			m.effectiveConnection(),
			m.effectiveSchema(),
			path,
			strings.TrimSpace(m.execPattern.Value()),
			parseVarLines(m.execVars.Value()),
			m.execDryRun,
			m.execTransaction,
			!m.execContinueOnErr,
		)
	}
	return m, nil
}

func (m *model) handleFormKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.formOpen = false
		m.status = "Form cancelled"
		return m, nil
	case "tab":
		m.formFieldIndex = (m.formFieldIndex + 1) % len(m.formFields)
		m.focusForm()
		return m, nil
	case "shift+tab":
		m.formFieldIndex--
		if m.formFieldIndex < 0 {
			m.formFieldIndex = len(m.formFields) - 1
		}
		m.focusForm()
		return m, nil
	case "ctrl+s":
		name := strings.TrimSpace(m.formFields[0].Value())
		if name == "" {
			m.status = "Connection name is required"
			return m, nil
		}
		conn := config.Connection{
			URL:      strings.TrimSpace(m.formFields[1].Value()),
			Host:     strings.TrimSpace(m.formFields[2].Value()),
			Port:     parseInt(m.formFields[3].Value(), 5432),
			Database: strings.TrimSpace(m.formFields[4].Value()),
			User:     strings.TrimSpace(m.formFields[5].Value()),
			Password: m.formFields[6].Value(),
			SSLMode:  defaultString(strings.TrimSpace(m.formFields[7].Value()), "require"),
		}
		m.busy = true
		return m, saveConnectionCmd(m.formEditingOld, name, conn)
	}

	var cmd tea.Cmd
	m.formFields[m.formFieldIndex], cmd = m.formFields[m.formFieldIndex].Update(msg)
	return m, cmd
}

func (m *model) handleSchemaSwitchKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.schemaSwitchOpen = false
		m.schemaSwitchInput.Blur()
		m.status = "Schema switch cancelled"
		return m, nil
	case "enter":
		m.currentSchema = defaultString(strings.TrimSpace(m.schemaSwitchInput.Value()), "public")
		m.schemaSwitchOpen = false
		m.schemaSwitchInput.Blur()
		m.status = "Current schema: " + m.currentSchema
		return m, nil
	}

	var cmd tea.Cmd
	m.schemaSwitchInput, cmd = m.schemaSwitchInput.Update(msg)
	return m, cmd
}

func (m *model) handleDataInspectKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "enter", "v", "q":
		m.dataInspectOpen = false
		m.status = m.dataStatus()
		return m, nil
	case "up", "k":
		m.dataInspect.LineUp(1)
		return m, nil
	case "down", "j":
		m.dataInspect.LineDown(1)
		return m, nil
	case "pgup":
		m.dataInspect.HalfViewUp()
		return m, nil
	case "pgdown":
		m.dataInspect.HalfViewDown()
		return m, nil
	case "home", "g":
		m.dataInspect.GotoTop()
		return m, nil
	case "end", "G":
		m.dataInspect.GotoBottom()
		return m, nil
	}

	var cmd tea.Cmd
	m.dataInspect, cmd = m.dataInspect.Update(msg)
	return m, cmd
}

func (m *model) handleDataPresetSaveKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q":
		m.dataPresetSaveOpen = false
		m.dataPresetName.Blur()
		m.status = "Preset save cancelled"
		return m, nil
	case "enter":
		name := strings.TrimSpace(m.dataPresetName.Value())
		if name == "" {
			m.status = "Preset name is required"
			return m, nil
		}
		tableKey := m.currentDataTableKey()
		if tableKey == "" {
			m.status = "Choose table first"
			return m, nil
		}
		m.busy = true
		return m, saveDataPresetCmd(tableKey, m.currentDataPreset(name))
	}

	var cmd tea.Cmd
	m.dataPresetName, cmd = m.dataPresetName.Update(msg)
	return m, cmd
}

func (m *model) handleDataPresetListKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	presets := m.dataPresetsForSelectedTable()
	switch msg.String() {
	case "esc", "q", "p":
		m.dataPresetListOpen = false
		m.status = m.dataStatus()
		return m, nil
	case "up", "k":
		if m.dataPresetIndex > 0 {
			m.dataPresetIndex--
		}
		return m, nil
	case "down", "j":
		if m.dataPresetIndex < len(presets)-1 {
			m.dataPresetIndex++
		}
		return m, nil
	case "d":
		if len(presets) == 0 {
			m.status = "No presets to delete"
			return m, nil
		}
		m.busy = true
		return m, deleteDataPresetCmd(m.currentDataTableKey(), presets[m.dataPresetIndex].Name)
	case "enter":
		if len(presets) == 0 {
			m.status = "No presets to apply"
			return m, nil
		}
		m.applyDataPreset(presets[m.dataPresetIndex])
		m.dataPresetListOpen = false
		m.busy = true
		return m, runDataPreviewCmd(
			m.opts,
			m.effectiveConnection(),
			m.effectiveSchema(),
			m.selectedTable,
			m.dataPageSize,
			m.dataSortCol,
			m.dataSortDesc,
			m.dataFilters,
		)
	}
	return m, nil
}

func (m *model) handleSchemaTablePickerKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	filtered := m.filteredSchemaTables()

	switch msg.String() {
	case "esc":
		m.schemaTablePickerOpen = false
		m.schemaTableFilter.Blur()
		m.status = "Table picker cancelled"
		return m, nil
	case "up", "k":
		if m.schemaTableIndex > 0 {
			m.schemaTableIndex--
		}
		return m, nil
	case "down", "j":
		if m.schemaTableIndex < len(filtered)-1 {
			m.schemaTableIndex++
		}
		return m, nil
	case "enter":
		if len(filtered) == 0 {
			m.status = "No tables match filter"
			return m, nil
		}
		m.selectedTable = filtered[m.schemaTableIndex]
		m.schemaTablePickerOpen = false
		m.schemaTableFilter.Blur()
		m.status = "Studying table: " + m.selectedTable
		return m, nil
	}

	var cmd tea.Cmd
	m.schemaTableFilter, cmd = m.schemaTableFilter.Update(msg)
	filtered = m.filteredSchemaTables()
	if len(filtered) == 0 {
		m.schemaTableIndex = 0
	} else if m.schemaTableIndex >= len(filtered) {
		m.schemaTableIndex = len(filtered) - 1
	}
	return m, cmd
}

func (m *model) runSchemaAction() (tea.Model, tea.Cmd) {
	actions := m.visibleSchemaActions()
	if len(actions) == 0 {
		return m, nil
	}
	if m.schemaIndex >= len(actions) {
		m.schemaIndex = len(actions) - 1
	}
	action := actions[m.schemaIndex]
	arg := strings.TrimSpace(m.schemaArg.Value())
	name := m.effectiveConnection()
	if name == "" && m.opts.URL == "" {
		m.status = "Select a connection first"
		return m, nil
	}

	m.busy = true
	switch action {
	case "Tables":
		return m, loadSchemaTablesCmd(m.opts, name, "")
	case "Describe":
		if m.selectedTable == "" {
			m.status = "Choose table first"
			m.busy = false
			return m, nil
		}
		return m, runSchemaCmd(m.opts, name, m.effectiveSchema(), "Describe "+m.selectedTable, db.DescribeTableSQL(), m.selectedTable)
	case "Columns":
		if m.selectedTable == "" {
			m.status = "Choose table first"
			m.busy = false
			return m, nil
		}
		return m, runSchemaCmd(m.opts, name, m.effectiveSchema(), "Columns "+m.selectedTable, db.ColumnConstraintsSQL(), m.selectedTable)
	case "Privileges":
		if m.selectedTable == "" {
			m.status = "Choose table first"
			m.busy = false
			return m, nil
		}
		return m, runSchemaCmd(m.opts, name, m.effectiveSchema(), "Privileges "+m.selectedTable, db.TablePrivilegesSQL(), m.selectedTable)
	case "Data":
		if m.selectedTable == "" {
			m.status = "Choose table first"
			m.busy = false
			return m, nil
		}
		limit, filter, excludes := parseDataArgs(arg, 100)
		m.dataPageSize = max(100, limit)
		if column := m.selectedDataColumnName(); column != "" {
			m.upsertDataFilter(column, filter, excludes)
		}
		return m, runDataPreviewCmd(
			m.opts,
			name,
			m.effectiveSchema(),
			m.selectedTable,
			m.dataPageSize,
			m.dataSortCol,
			m.dataSortDesc,
			m.dataFilters,
		)
	case "Indexes":
		return m, runSchemaCmd(m.opts, name, m.effectiveSchema(), "Indexes", db.IndexesSQL(arg != ""), anyOrNone(arg)...)
	case "Users":
		return m, runSchemaCmd(m.opts, name, m.effectiveSchema(), "Users", db.UsersSQL())
	case "Sizes":
		top := parseInt(arg, 20)
		return m, runSchemaCmd(m.opts, name, m.effectiveSchema(), "Sizes", db.SizesSQL(), top)
	case "FKeys":
		if m.selectedTable == "" {
			m.status = "Choose table first"
			m.busy = false
			return m, nil
		}
		return m, runSchemaCmd(m.opts, name, m.effectiveSchema(), "FKeys "+m.selectedTable, db.FKeysSQL(), m.selectedTable)
	case "Enums":
		return m, runSchemaCmd(m.opts, name, m.effectiveSchema(), "Enums", db.EnumsSQL())
	case "Activity":
		return m, runSchemaCmd(m.opts, name, m.effectiveSchema(), "Activity", db.ActivitySQL())
	case "Locks":
		return m, runSchemaCmd(m.opts, name, m.effectiveSchema(), "Locks", db.LocksSQL())
	default:
		m.busy = false
		return m, nil
	}
}

func (m *model) resize() {
	bodyHeight := m.bodyHeight()
	rightWidth := max(40, m.width-36)

	m.detail.Width = rightWidth
	m.detail.Height = max(6, bodyHeight-16)
	m.queryResult.Width = m.width - 4
	m.queryResult.Height = max(6, bodyHeight/2-6)
	m.execResult.Width = m.width - 4
	m.execResult.Height = max(6, bodyHeight-18)

	m.queryEditor.SetWidth(max(20, m.width-6))
	m.queryEditor.SetHeight(max(6, bodyHeight/2-4))
	m.execVars.SetWidth(max(20, m.width-6))
	m.execVars.SetHeight(4)
	m.schemaArg.Width = max(18, min(26, m.width-40))
	m.execPath.Width = max(24, m.width-8)
	m.execPattern.Width = max(16, min(40, m.width-8))
	m.dataInspect.Width = max(24, min(100, m.width-10))
	m.dataInspect.Height = max(8, m.height-12)
	m.dataPresetName.Width = max(24, min(48, m.width-12))

	for i := range m.formFields {
		m.formFields[i].Width = max(24, min(56, m.width-14))
	}
}

func (m *model) visibleSchemaActions() []string {
	actions := []string{"Tables", "Indexes", "Users", "Sizes", "Enums", "Activity", "Locks"}
	if m.selectedTable != "" {
		actions = append(actions, "Describe", "Columns", "Privileges", "Data", "FKeys")
	}
	return actions
}

func (m *model) clearDataPreview() {
	m.dataColumns = nil
	m.dataRows = nil
	m.dataRowOffset = 0
	m.dataColOffset = 0
	m.dataFocus = false
	m.dataPinnedCol = ""
	m.dataSelectedRow = 0
	m.dataSelectedCol = 0
	m.dataPageSize = 0
	m.dataLoadingMore = false
	m.dataEOF = false
	m.dataSortCol = ""
	m.dataSortDesc = false
	m.dataFilters = nil
	m.dataInspectOpen = false
	m.dataInspect.SetContent("")
	m.dataPresetSaveOpen = false
	m.dataPresetListOpen = false
	m.dataPresetName.SetValue("")
	m.dataPresetName.Blur()
	m.dataPresetIndex = 0
}

func (m model) hasDataPreview() bool {
	return len(m.dataColumns) > 0
}

func (m *model) filteredSchemaTables() []string {
	filter := strings.ToLower(strings.TrimSpace(m.schemaTableFilter.Value()))
	if filter == "" {
		return append([]string(nil), m.schemaTableNames...)
	}

	filtered := make([]string, 0, len(m.schemaTableNames))
	for _, table := range m.schemaTableNames {
		if strings.Contains(strings.ToLower(table), filter) {
			filtered = append(filtered, table)
		}
	}
	return filtered
}

func (m *model) isTableListAction() bool {
	actions := m.visibleSchemaActions()
	if len(actions) == 0 || m.schemaIndex >= len(actions) {
		return false
	}
	return actions[m.schemaIndex] == "Tables"
}

func (m *model) isDataAction() bool {
	actions := m.visibleSchemaActions()
	if len(actions) == 0 || m.schemaIndex >= len(actions) {
		return false
	}
	return actions[m.schemaIndex] == "Data"
}

func (m *model) showingDataPreview() bool {
	return m.hasDataPreview() && (m.dataFocus || m.isDataAction())
}

func (m *model) schemaTableDisplayLines(width int) ([]string, int) {
	lines := []string{
		"Use right/tab to focus list, Enter opens columns",
		"",
	}
	selectedLine := 0

	currentSchema := ""
	for i, table := range m.schemaTableNames {
		schema, name := splitQualifiedTable(table)
		if schema != currentSchema {
			currentSchema = schema
			lines = append(lines, "["+schema+"]")
		}

		prefix := "  "
		if m.schemaTableFocus && i == m.schemaTableIndex {
			prefix = "> "
			selectedLine = len(lines)
		}
		if !m.schemaTableFocus && schema == m.effectiveSchema() && name == m.selectedTable {
			prefix = "* "
			selectedLine = len(lines)
		}
		lines = append(lines, prefix+shorten(name, width-2))
	}

	return lines, selectedLine
}

func (m *model) viewSchemaTablesList(width int, height int) string {
	if len(m.schemaTableNames) == 0 {
		return "Press Enter to load tables"
	}

	lines, selectedLine := m.schemaTableDisplayLines(width)
	maxVisible := max(6, height-4)
	if len(lines) <= maxVisible {
		return strings.Join(lines, "\n")
	}

	start := 0
	if selectedLine >= maxVisible {
		start = selectedLine - maxVisible/2
	}
	if start+maxVisible > len(lines) {
		start = len(lines) - maxVisible
	}
	if start < 0 {
		start = 0
	}

	end := min(len(lines), start+maxVisible)
	visible := append([]string(nil), lines[start:end]...)
	if start > 0 {
		visible[0] = "..."
	}
	if end < len(lines) {
		visible[len(visible)-1] = "..."
	}

	return strings.Join(visible, "\n")
}

func (m model) viewDataGrid(width int, height int) string {
	if !m.hasDataPreview() {
		return "Press Enter to load data preview"
	}

	m.normalizeDataState()
	rowOffset := min(m.dataRowOffset, m.maxDataRowOffset())
	rowCapacity := m.dataRowCapacity()
	endRow := min(len(m.dataRows), rowOffset+rowCapacity)
	rowLabelWidth := max(3, len(strconv.Itoa(max(1, len(m.dataRows)))))
	pinnedIndex := m.pinnedDataColumnIndex()
	columnIndexes, columnWidths := m.visibleDataColumns(width, rowLabelWidth, pinnedIndex)
	if len(columnIndexes) == 0 {
		return "Terminal is too narrow for data preview"
	}

	headerCells := make([]string, 0, len(columnIndexes))
	for i, columnIndex := range columnIndexes {
		selectedColumn := columnIndex == m.dataSelectedCol
		pinnedColumn := columnIndex == pinnedIndex
		headerCells = append(headerCells, m.renderDataCell(m.dataColumns[columnIndex], columnWidths[i], false, selectedColumn, pinnedColumn))
	}

	lines := []string{
		fmt.Sprintf(
			"rows %d-%d/%d loaded  col:%s  pin:%s  sort:%s  find:%s  %s",
			min(len(m.dataRows), rowOffset+1),
			endRow,
			len(m.dataRows),
			displayOr(m.selectedDataColumnName(), "-"),
			displayOr(m.activePinnedColumnName(), "-"),
			displayOr(m.dataSortLabel(), "-"),
			displayOr(m.dataFilterLabel(), "-"),
			m.dataFocusHelp(),
		),
		buildDataLine("#", rowLabelWidth, headerCells),
		buildDataSeparator(rowLabelWidth, columnWidths),
	}

	for rowIndex := rowOffset; rowIndex < endRow; rowIndex++ {
		rowCells := make([]string, 0, len(columnIndexes))
		for i, columnIndex := range columnIndexes {
			value := ""
			if columnIndex < len(m.dataRows[rowIndex]) {
				value = m.dataRows[rowIndex][columnIndex]
			}
			selectedRow := rowIndex == m.dataSelectedRow
			selectedColumn := columnIndex == m.dataSelectedCol
			pinnedColumn := columnIndex == pinnedIndex
			rowCells = append(rowCells, m.renderDataCell(value, columnWidths[i], selectedRow, selectedColumn, pinnedColumn))
		}
		rowLabel := padLeft(strconv.Itoa(rowIndex+1), rowLabelWidth)
		if rowIndex == m.dataSelectedRow {
			rowLabel = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("230")).Background(lipgloss.Color("62")).Render(rowLabel)
		}
		lines = append(lines, buildDataLine(rowLabel, rowLabelWidth, rowCells))
	}

	if len(m.dataRows) == 0 {
		lines = append(lines, "(no rows)")
	}

	if len(lines) > height {
		lines = lines[:height]
	}

	return strings.Join(lines, "\n")
}

func (m model) dataFocusHelp() string {
	if m.dataFocus {
		return "arrows move, o sort, a search, f pin, r reset, tab back"
	}
	return "right/tab focus data"
}

func (m model) dataRowCapacity() int {
	return max(1, m.bodyHeight()-7)
}

func (m model) maxDataRowOffset() int {
	return max(0, len(m.dataRows)-m.dataRowCapacity())
}

func (m model) visibleDataColumns(width int, rowLabelWidth int, pinnedIndex int) ([]int, []int) {
	available := max(8, width-rowLabelWidth-3)
	indexes := make([]int, 0)
	widths := make([]int, 0)
	used := 0

	if pinnedIndex >= 0 && pinnedIndex < len(m.dataColumns) {
		pinnedWidth := m.dataColumnWidth(pinnedIndex)
		indexes = append(indexes, pinnedIndex)
		widths = append(widths, pinnedWidth)
		used += pinnedWidth
	}

	for i := m.dataColOffset; i < len(m.dataColumns); i++ {
		if i == pinnedIndex {
			continue
		}
		columnWidth := m.dataColumnWidth(i)
		extra := columnWidth
		if len(indexes) > 0 {
			extra += 3
		}
		if used+extra > available {
			if len(indexes) == 0 {
				indexes = append(indexes, i)
				widths = append(widths, max(4, available))
			}
			break
		}
		indexes = append(indexes, i)
		widths = append(widths, columnWidth)
		used += extra
	}

	return indexes, widths
}

func (m model) dataColumnWidth(index int) int {
	width := max(4, runeLen(m.dataColumns[index]))
	for _, row := range m.dataRows {
		if index >= len(row) {
			continue
		}
		width = max(width, runeLen(row[index]))
	}
	return min(width, 24)
}

func (m *model) normalizeDataState() {
	if len(m.dataColumns) == 0 {
		m.dataSelectedCol = 0
		m.dataColOffset = 0
	} else {
		m.dataSelectedCol = min(max(0, m.dataSelectedCol), len(m.dataColumns)-1)
	}
	if len(m.dataRows) == 0 {
		m.dataSelectedRow = 0
		m.dataRowOffset = 0
	} else {
		m.dataSelectedRow = min(max(0, m.dataSelectedRow), len(m.dataRows)-1)
	}
	m.ensureDataSelectionVisible()
}

func (m *model) ensureDataSelectionVisible() {
	if len(m.dataRows) > 0 {
		if m.dataSelectedRow < m.dataRowOffset {
			m.dataRowOffset = m.dataSelectedRow
		}
		if m.dataSelectedRow >= m.dataRowOffset+m.dataRowCapacity() {
			m.dataRowOffset = m.dataSelectedRow - m.dataRowCapacity() + 1
		}
		m.dataRowOffset = min(max(0, m.dataRowOffset), m.maxDataRowOffset())
	}
	if len(m.dataColumns) == 0 {
		m.dataColOffset = 0
		return
	}
	pinnedIndex := m.pinnedDataColumnIndex()
	if m.dataSelectedCol == pinnedIndex {
		return
	}
	if m.dataSelectedCol < m.dataColOffset {
		m.dataColOffset = m.dataSelectedCol
	}
	gridWidth := m.dataGridWidth()
	for attempts := 0; attempts < len(m.dataColumns); attempts++ {
		visible, _ := m.visibleDataColumns(gridWidth, max(3, len(strconv.Itoa(max(1, len(m.dataRows))))), pinnedIndex)
		if containsInt(visible, m.dataSelectedCol) {
			return
		}
		if m.dataSelectedCol > m.dataColOffset {
			m.dataColOffset++
			continue
		}
		m.dataColOffset = max(0, m.dataSelectedCol)
	}
}

func (m model) dataGridWidth() int {
	_, rightWidth, _ := m.splitLayoutWidths(30, 40)
	return rightWidth - 4
}

func (m *model) maybeLoadMoreData() tea.Cmd {
	if m.dataLoadingMore || m.dataEOF || !m.hasDataPreview() {
		return nil
	}
	if m.dataSelectedRow < len(m.dataRows)-1 {
		return nil
	}
	m.dataLoadingMore = true
	return loadMoreDataCmd(
		m.opts,
		m.effectiveConnection(),
		m.effectiveSchema(),
		m.selectedTable,
		len(m.dataRows),
		max(100, m.dataPageSize),
		m.dataSortCol,
		m.dataSortDesc,
		m.dataFilters,
	)
}

func (m model) selectedDataColumnName() string {
	if m.dataSelectedCol < 0 || m.dataSelectedCol >= len(m.dataColumns) {
		return ""
	}
	return m.dataColumns[m.dataSelectedCol]
}

func (m model) dataStatus() string {
	if !m.hasDataPreview() {
		return "Data preview"
	}
	row := 0
	if len(m.dataRows) > 0 {
		row = m.dataSelectedRow + 1
	}
	status := fmt.Sprintf("Data row %d/%d col %s", row, len(m.dataRows), displayOr(m.selectedDataColumnName(), "-"))
	if m.dataLoadingMore {
		status += " | loading more rows..."
	}
	return status
}

func (m *model) upsertDataFilter(column string, include string, excludes []string) {
	column = strings.TrimSpace(column)
	include = strings.TrimSpace(include)
	cleanExcludes := make([]string, 0, len(excludes))
	for _, exclude := range excludes {
		exclude = strings.TrimSpace(exclude)
		if exclude != "" {
			cleanExcludes = append(cleanExcludes, exclude)
		}
	}

	filterIndex := -1
	for i, filter := range m.dataFilters {
		if strings.EqualFold(filter.Column, column) {
			filterIndex = i
			break
		}
	}

	if include == "" && len(cleanExcludes) == 0 {
		if filterIndex >= 0 {
			m.dataFilters = append(m.dataFilters[:filterIndex], m.dataFilters[filterIndex+1:]...)
		}
		return
	}

	filter := db.PreviewFilter{
		Column:   column,
		Include:  include,
		Excludes: cleanExcludes,
	}
	if filterIndex >= 0 {
		m.dataFilters[filterIndex] = filter
		return
	}
	m.dataFilters = append(m.dataFilters, filter)
}

func (m model) formatDataFilter(filter db.PreviewFilter) string {
	parts := make([]string, 0, 1+len(filter.Excludes))
	if strings.TrimSpace(filter.Include) != "" {
		parts = append(parts, filter.Include)
	}
	for _, exclude := range filter.Excludes {
		if strings.TrimSpace(exclude) != "" {
			parts = append(parts, "-"+exclude)
		}
	}
	return filter.Column + ": " + strings.Join(parts, " ")
}

func (m model) currentDataTableKey() string {
	if m.selectedTable == "" {
		return ""
	}
	return m.effectiveSchema() + "." + m.selectedTable
}

func (m model) dataPresetsForSelectedTable() []config.DataPreset {
	if m.cfg == nil || m.cfg.DataPresets == nil {
		return nil
	}
	tableKey := m.currentDataTableKey()
	presets := append([]config.DataPreset(nil), m.cfg.DataPresets[tableKey]...)
	sort.SliceStable(presets, func(i, j int) bool {
		return strings.ToLower(presets[i].Name) < strings.ToLower(presets[j].Name)
	})
	return presets
}

func (m model) currentDataPreset(name string) config.DataPreset {
	return config.DataPreset{
		Name:         name,
		PageSize:     max(100, m.dataPageSize),
		SortColumn:   m.dataSortCol,
		SortDesc:     m.dataSortDesc,
		PinnedColumn: m.dataPinnedCol,
		Filters:      toConfigFilters(m.dataFilters),
	}
}

func (m *model) applyDataPreset(preset config.DataPreset) {
	m.dataPageSize = max(100, preset.PageSize)
	m.dataSortCol = preset.SortColumn
	m.dataSortDesc = preset.SortDesc
	m.dataPinnedCol = preset.PinnedColumn
	m.dataFilters = fromConfigFilters(preset.Filters)
	m.dataRowOffset = 0
	m.dataColOffset = 0
	m.dataSelectedRow = 0
	m.dataEOF = false
	m.dataLoadingMore = false
	m.dataInspectOpen = false
	m.schemaArg.SetValue("")
	if input := m.selectedColumnFilterInput(); input != "" {
		m.schemaArg.SetValue(input)
	}
	m.status = "Applied preset: " + preset.Name
}

func (m model) selectedColumnFilterInput() string {
	column := m.selectedDataColumnName()
	if column == "" {
		return ""
	}
	for _, filter := range m.dataFilters {
		if !strings.EqualFold(filter.Column, column) {
			continue
		}
		parts := make([]string, 0, 1+len(filter.Excludes))
		if strings.TrimSpace(filter.Include) != "" {
			parts = append(parts, filter.Include)
		}
		for _, exclude := range filter.Excludes {
			if strings.TrimSpace(exclude) != "" {
				parts = append(parts, "-"+exclude)
			}
		}
		return strings.Join(parts, " ")
	}
	return ""
}

func (m *model) removeDataFilter(column string) bool {
	for i, filter := range m.dataFilters {
		if !strings.EqualFold(filter.Column, column) {
			continue
		}
		m.dataFilters = append(m.dataFilters[:i], m.dataFilters[i+1:]...)
		return true
	}
	return false
}

func toConfigFilters(filters []db.PreviewFilter) []config.DataPresetFilter {
	out := make([]config.DataPresetFilter, 0, len(filters))
	for _, filter := range filters {
		out = append(out, config.DataPresetFilter{
			Column:   filter.Column,
			Include:  filter.Include,
			Excludes: append([]string(nil), filter.Excludes...),
		})
	}
	return out
}

func fromConfigFilters(filters []config.DataPresetFilter) []db.PreviewFilter {
	out := make([]db.PreviewFilter, 0, len(filters))
	for _, filter := range filters {
		out = append(out, db.PreviewFilter{
			Column:   filter.Column,
			Include:  filter.Include,
			Excludes: append([]string(nil), filter.Excludes...),
		})
	}
	return out
}

func (m *model) openDataInspect() {
	m.dataInspectOpen = true
	m.dataInspect.GotoTop()
	m.dataInspect.SetContent(m.dataInspectContent())
	m.status = "Cell details"
}

func (m model) dataInspectContent() string {
	if len(m.dataRows) == 0 || len(m.dataColumns) == 0 {
		return "No data selected."
	}
	rowIndex := min(max(0, m.dataSelectedRow), len(m.dataRows)-1)
	colIndex := min(max(0, m.dataSelectedCol), len(m.dataColumns)-1)
	row := m.dataRows[rowIndex]
	column := m.dataColumns[colIndex]
	value := ""
	if colIndex < len(row) {
		value = row[colIndex]
	}

	lines := []string{
		fmt.Sprintf("Row: %d", rowIndex+1),
		fmt.Sprintf("Column: %s", column),
		"",
		"Selected value",
		value,
		"",
		"Full row",
	}
	for i, name := range m.dataColumns {
		cell := ""
		if i < len(row) {
			cell = row[i]
		}
		lines = append(lines, name+":")
		lines = append(lines, cell)
		lines = append(lines, "")
	}
	return strings.Join(lines, "\n")
}

func (m model) dataSortLabel() string {
	if m.dataSortCol == "" {
		return ""
	}
	direction := "asc"
	if m.dataSortDesc {
		direction = "desc"
	}
	return m.dataSortCol + " " + direction
}

func (m model) dataFilterLabel() string {
	if len(m.dataFilters) == 0 {
		return ""
	}
	parts := make([]string, 0, len(m.dataFilters))
	for _, filter := range m.dataFilters {
		label := filter.Column + "~"
		chunks := make([]string, 0, 1+len(filter.Excludes))
		if strings.TrimSpace(filter.Include) != "" {
			chunks = append(chunks, filter.Include)
		}
		for _, exclude := range filter.Excludes {
			if strings.TrimSpace(exclude) != "" {
				chunks = append(chunks, "-"+exclude)
			}
		}
		label += strings.Join(chunks, " ")
		parts = append(parts, label)
	}
	return shorten(strings.Join(parts, "; "), 48)
}

func (m model) renderDataCell(value string, width int, selectedRow bool, selectedColumn bool, pinnedColumn bool) string {
	content := padRight(shorten(value, width), width)
	style := lipgloss.NewStyle()
	switch {
	case selectedRow && selectedColumn:
		style = style.Bold(true).Foreground(lipgloss.Color("230")).Background(lipgloss.Color("62"))
	case selectedColumn:
		style = style.Bold(true).Foreground(lipgloss.Color("230")).Background(lipgloss.Color("60"))
	case selectedRow:
		style = style.Foreground(lipgloss.Color("230")).Background(lipgloss.Color("238"))
	case pinnedColumn:
		style = style.Foreground(lipgloss.Color("151"))
	}
	return style.Render(content)
}

func (m model) pinnedDataColumnIndex() int {
	if m.dataPinnedCol == "" {
		return -1
	}
	for i, column := range m.dataColumns {
		if column == m.dataPinnedCol || strings.EqualFold(column, m.dataPinnedCol) {
			return i
		}
	}
	return -1
}

func (m model) activePinnedColumnName() string {
	index := m.pinnedDataColumnIndex()
	if index < 0 || index >= len(m.dataColumns) {
		return ""
	}
	return m.dataColumns[index]
}

func (m *model) refreshConnectionNames() {
	m.connectionNames = m.connectionNames[:0]
	for name := range m.cfg.Connections {
		m.connectionNames = append(m.connectionNames, name)
	}
	sort.Strings(m.connectionNames)

	if len(m.connectionNames) == 0 {
		m.connectionIndex = 0
		if m.currentConnection != "" && m.opts.URL == "" {
			m.currentConnection = ""
		}
		return
	}

	if m.connectionIndex >= len(m.connectionNames) {
		m.connectionIndex = len(m.connectionNames) - 1
	}

	if m.currentConnection == "" && m.cfg.Default != "" {
		m.currentConnection = m.cfg.Default
	}
}

func (m *model) selectedConnectionName() string {
	if len(m.connectionNames) == 0 {
		return ""
	}
	return m.connectionNames[m.connectionIndex]
}

func (m *model) effectiveConnection() string {
	if m.currentConnection != "" {
		return m.currentConnection
	}
	if m.cfg.Default != "" {
		return m.cfg.Default
	}
	return m.selectedConnectionName()
}

func (m *model) effectiveSchema() string {
	return defaultString(strings.TrimSpace(m.currentSchema), "public")
}

func (m *model) openForm(oldName string, conn config.Connection) {
	fields := make([]textinput.Model, 8)
	placeholders := []string{"name", "postgres://...", "host", "5432", "database", "user", "password", "require"}
	values := []string{
		oldName,
		conn.URL,
		conn.Host,
		portValue(conn.Port),
		conn.Database,
		conn.User,
		conn.Password,
		defaultString(conn.SSLMode, "require"),
	}

	for i := range fields {
		fields[i] = textinput.New()
		fields[i].Placeholder = placeholders[i]
		fields[i].SetValue(values[i])
		fields[i].CharLimit = 512
		fields[i].Width = max(24, min(56, m.width-14))
	}

	m.formOpen = true
	m.formEditingOld = oldName
	m.formFields = fields
	m.formFieldIndex = 0
	m.focusForm()
}

func (m *model) focusForm() {
	for i := range m.formFields {
		if i == m.formFieldIndex {
			m.formFields[i].Focus()
		} else {
			m.formFields[i].Blur()
		}
	}
}

func (m *model) focusExec() {
	m.execPath.Blur()
	m.execPattern.Blur()
	m.execVars.Blur()
	switch m.execFocus {
	case 0:
		m.execPath.Focus()
	case 1:
		m.execPattern.Focus()
	case 2:
		m.execVars.Focus()
	}
}

func (m model) viewHeader() string {
	tabNames := []string{"1 Connections", "2 Schema", "3 Query", "4 Exec"}
	var rendered []string
	for i, name := range tabNames {
		style := lipgloss.NewStyle().Padding(0, 1)
		if int(m.activeTab) == i {
			style = style.Bold(true).Foreground(lipgloss.Color("230")).Background(lipgloss.Color("62"))
		} else {
			style = style.Foreground(lipgloss.Color("245"))
		}
		rendered = append(rendered, style.Render(name))
	}

	conn := m.effectiveConnection()
	if conn == "" && m.opts.URL != "" {
		conn = "adhoc-url"
	}
	if conn == "" {
		conn = "none"
	}

	info := lipgloss.NewStyle().Foreground(lipgloss.Color("109")).Render("Current: " + conn + " | Schema: " + m.effectiveSchema())
	return lipgloss.JoinHorizontal(lipgloss.Top, strings.Join(rendered, " "), "   ", info)
}

func (m model) viewBody() string {
	switch m.activeTab {
	case tabConnections:
		return m.viewConnections()
	case tabSchema:
		return m.viewSchema()
	case tabQuery:
		return m.viewQuery()
	case tabExec:
		return m.viewExec()
	default:
		return ""
	}
}

func (m model) viewConnections() string {
	leftWidth, rightWidth, stacked := m.splitLayoutWidths(32, 40)
	bodyHeight := m.bodyHeight()
	listStyle := lipgloss.NewStyle().Width(leftWidth).Height(bodyHeight).Border(lipgloss.RoundedBorder()).Padding(0, 1)
	detailStyle := lipgloss.NewStyle().Width(rightWidth).Height(bodyHeight).Border(lipgloss.RoundedBorder()).Padding(0, 1)

	lines := []string{"Connections"}
	if len(m.connectionNames) == 0 {
		lines = append(lines, "", "No saved connections", "", "Press n to add one")
	} else {
		for i, name := range m.connectionNames {
			prefix := "  "
			if i == m.connectionIndex {
				prefix = "> "
			}
			markers := []string{}
			if name == m.cfg.Default {
				markers = append(markers, "D")
			}
			if name == m.currentConnection {
				markers = append(markers, "C")
			}
			label := name
			if len(markers) > 0 {
				label += " [" + strings.Join(markers, ",") + "]"
			}
			lines = append(lines, prefix+label)
		}
	}

	right := []string{"Details"}
	if name := m.selectedConnectionName(); name != "" {
		conn := m.cfg.Connections[name]
		right = append(right,
			"",
			"Name: "+shorten(name, rightWidth-10),
			"Host: "+shorten(displayOr(conn.Host, "-"), rightWidth-10),
			"Port: "+displayOr(intString(conn.Port), "-"),
			"DB: "+shorten(displayOr(conn.Database, "-"), rightWidth-8),
			"User: "+shorten(displayOr(conn.User, "-"), rightWidth-10),
			"SSL: "+shorten(displayOr(conn.SSLMode, "-"), rightWidth-9),
		)
		if conn.URL != "" {
			right = append(right, "URL: "+shorten(conn.URL, rightWidth-9))
		}
	}
	if m.detail.View() != "" {
		right = append(right, "", "Last output", "", clampBlock(m.detail.View(), rightWidth-4))
	}

	leftPane := listStyle.Render(strings.Join(lines, "\n"))
	rightPane := detailStyle.Render(strings.Join(right, "\n"))
	if stacked {
		return lipgloss.JoinVertical(lipgloss.Left, leftPane, rightPane)
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, leftPane, rightPane)
}

func (m model) viewSchema() string {
	leftWidth, rightWidth, stacked := m.splitLayoutWidths(30, 40)
	bodyHeight := m.bodyHeight()
	actions := m.visibleSchemaActions()
	if m.schemaIndex >= len(actions) {
		m.schemaIndex = max(0, len(actions)-1)
	}
	left := []string{"Schema Actions", ""}
	for i, action := range actions {
		prefix := "  "
		if i == m.schemaIndex {
			prefix = "> "
		}
		left = append(left, prefix+action)
	}
	left = append(left, "")
	if m.selectedTable != "" {
		left = append(left, "Studying", shorten(m.selectedTable, leftWidth-4))
	} else {
		left = append(left, "Studying", "-")
	}
	if m.showingDataPreview() {
		left = append(left, "", "Pinned", shorten(displayOr(m.dataPinnedCol, "-"), leftWidth-4))
		left = append(left, "Sort", shorten(displayOr(m.dataSortLabel(), "-"), leftWidth-4))
		if len(m.dataFilters) == 0 {
			left = append(left, "Filters", "-")
		} else {
			left = append(left, "Filters", shorten(fmt.Sprintf("%d active", len(m.dataFilters)), leftWidth-4))
			for _, filter := range m.dataFilters {
				left = append(left, shorten(m.formatDataFilter(filter), leftWidth-4))
			}
		}
	}
	left = append(left, "", "Argument", m.schemaArg.View())

	rightTitle := "Result"
	if m.isTableListAction() {
		rightTitle = "Tables by schema"
	} else if m.showingDataPreview() {
		rightTitle = "Data preview"
		if m.hasDataPreview() {
			rightTitle = m.status
		}
	} else if m.detail.View() != "" {
		rightTitle = m.status
	}

	leftStyle := lipgloss.NewStyle().Width(leftWidth).Height(bodyHeight).Border(lipgloss.RoundedBorder()).Padding(0, 1)
	rightStyle := lipgloss.NewStyle().Width(rightWidth).Height(bodyHeight).Border(lipgloss.RoundedBorder()).Padding(0, 1)
	leftPane := leftStyle.Render(strings.Join(left, "\n"))
	rightContent := clampBlock(m.detail.View(), rightWidth-4)
	if m.isTableListAction() {
		rightContent = m.viewSchemaTablesList(rightWidth-4, bodyHeight-4)
	} else if m.showingDataPreview() {
		rightContent = m.viewDataGrid(rightWidth-4, bodyHeight-4)
	}
	rightPane := rightStyle.Render(rightTitle + "\n\n" + rightContent)
	if stacked {
		return lipgloss.JoinVertical(lipgloss.Left, leftPane, rightPane)
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, leftPane, rightPane)
}

func (m model) viewQuery() string {
	box := lipgloss.NewStyle().Width(m.width-2).Height(m.bodyHeight()).Border(lipgloss.RoundedBorder()).Padding(0, 1)
	content := "Editor\n\n" + m.queryEditor.View() + "\n\nResult\n\n" + clampBlock(m.queryResult.View(), m.width-8)
	return box.Render(content)
}

func (m model) viewExec() string {
	box := lipgloss.NewStyle().Width(m.width-2).Height(m.bodyHeight()).Border(lipgloss.RoundedBorder()).Padding(0, 1)
	flags := fmt.Sprintf("dry-run:%t  transaction:%t  continue-on-error:%t", m.execDryRun, m.execTransaction, m.execContinueOnErr)
	content := strings.Join([]string{
		"Path",
		m.execPath.View(),
		"",
		"Pattern",
		m.execPattern.View(),
		"",
		flags,
		"",
		"Vars",
		m.execVars.View(),
		"",
		"Result",
		clampBlock(m.execResult.View(), m.width-8),
	}, "\n")
	return box.Render(content)
}

func (m model) viewFooter() string {
	status := m.status
	if m.busy {
		status = "Working..."
	}

	help := ""
	switch m.activeTab {
	case tabConnections:
		help = "enter select  u default  t test  n new  e edit  d delete  s schema"
	case tabSchema:
		help = "enter load/run  arrows move data  w save-preset  p presets  v details  o sort  a search  d drop-filter  f pin  r reset  tab/esc/q back  s schema  x clear table"
	case tabQuery:
		help = "ctrl+e run  ctrl+k clear  s schema"
	case tabExec:
		help = "tab switch field  ctrl+e run  ctrl+r dry-run  ctrl+t tx  ctrl+g continue  s schema"
	}

	return lipgloss.NewStyle().Foreground(lipgloss.Color("245")).Render(status + "   |   " + help + "   |   ctrl+d/ctrl+c quit")
}

func (m model) viewForm() string {
	labels := []string{"Name", "URL", "Host", "Port", "Database", "User", "Password", "SSL mode"}
	lines := []string{"Connection Form", ""}
	for i, field := range m.formFields {
		lines = append(lines, labels[i]+":", field.View(), "")
	}
	lines = append(lines, "ctrl+s save, esc cancel, tab next")

	style := lipgloss.NewStyle().
		Border(lipgloss.DoubleBorder()).
		Padding(1, 2).
		Width(min(72, m.width-4))
	return style.Render(strings.Join(lines, "\n"))
}

func (m model) viewSchemaSwitchScreen() string {
	lines := []string{
		"Switch Schema",
		"",
		"Schema:",
		m.schemaSwitchInput.View(),
		"",
		"enter apply, esc cancel",
	}

	card := lipgloss.NewStyle().
		Border(lipgloss.DoubleBorder()).
		Padding(1, 2).
		Width(min(48, m.width-4)).
		Render(strings.Join(lines, "\n"))

	return lipgloss.NewStyle().
		Width(max(20, m.width)).
		Height(max(8, m.height-5)).
		Align(lipgloss.Center, lipgloss.Center).
		Render(card)
}

func (m model) viewDataPresetSaveScreen() string {
	lines := []string{
		"Save Data Preset",
		"",
		"Name:",
		m.dataPresetName.View(),
		"",
		"enter save, esc cancel",
	}

	card := lipgloss.NewStyle().
		Border(lipgloss.DoubleBorder()).
		Padding(1, 2).
		Width(min(56, m.width-4)).
		Render(strings.Join(lines, "\n"))

	return lipgloss.NewStyle().
		Width(max(20, m.width)).
		Height(max(8, m.height-5)).
		Align(lipgloss.Center, lipgloss.Center).
		Render(card)
}

func (m model) viewDataPresetListScreen() string {
	presets := m.dataPresetsForSelectedTable()
	lines := []string{
		"Data Presets",
		"",
		"Table: " + displayOr(m.currentDataTableKey(), "-"),
		"",
	}

	if len(presets) == 0 {
		lines = append(lines, "No saved presets for this table.")
	} else {
		limit := min(len(presets), max(8, m.height-14))
		start := 0
		if m.dataPresetIndex >= limit {
			start = m.dataPresetIndex - limit + 1
		}
		for i := start; i < min(start+limit, len(presets)); i++ {
			prefix := "  "
			if i == m.dataPresetIndex {
				prefix = "> "
			}
			lines = append(lines, prefix+presets[i].Name)
		}
	}

	lines = append(lines, "", "enter apply, d delete, esc cancel")

	card := lipgloss.NewStyle().
		Border(lipgloss.DoubleBorder()).
		Padding(1, 2).
		Width(min(64, m.width-4)).
		Render(strings.Join(lines, "\n"))

	return lipgloss.NewStyle().
		Width(max(20, m.width)).
		Height(max(8, m.height-5)).
		Align(lipgloss.Center, lipgloss.Center).
		Render(card)
}

func (m model) viewDataInspectScreen() string {
	title := "Cell details"
	if column := m.selectedDataColumnName(); column != "" {
		title = "Cell details: " + column
	}

	lines := []string{
		title,
		"",
		m.dataInspect.View(),
		"",
		"esc/enter/v close, arrows scroll",
	}

	card := lipgloss.NewStyle().
		Border(lipgloss.DoubleBorder()).
		Padding(1, 2).
		Width(max(28, min(104, m.width-6))).
		Height(max(10, min(m.height-6, m.dataInspect.Height+6))).
		Render(strings.Join(lines, "\n"))

	return lipgloss.NewStyle().
		Width(max(20, m.width)).
		Height(max(8, m.height-5)).
		Align(lipgloss.Center, lipgloss.Center).
		Render(card)
}

func (m model) viewSchemaTablePickerScreen() string {
	filtered := m.filteredSchemaTables()
	if m.schemaTableIndex >= len(filtered) && len(filtered) > 0 {
		m.schemaTableIndex = len(filtered) - 1
	}

	lines := []string{
		"Choose Table",
		"",
		"Filter:",
		m.schemaTableFilter.View(),
		"",
	}

	if len(filtered) == 0 {
		lines = append(lines, "No tables found")
	} else {
		limit := min(len(filtered), max(8, m.height-14))
		start := 0
		if m.schemaTableIndex >= limit {
			start = m.schemaTableIndex - limit + 1
		}
		for i := start; i < min(start+limit, len(filtered)); i++ {
			prefix := "  "
			if i == m.schemaTableIndex {
				prefix = "> "
			}
			lines = append(lines, prefix+filtered[i])
		}
	}

	lines = append(lines, "", "enter choose, esc cancel, type to filter")

	card := lipgloss.NewStyle().
		Border(lipgloss.DoubleBorder()).
		Padding(1, 2).
		Width(min(64, m.width-4)).
		Render(strings.Join(lines, "\n"))

	return lipgloss.NewStyle().
		Width(max(20, m.width)).
		Height(max(8, m.height-5)).
		Align(lipgloss.Center, lipgloss.Center).
		Render(card)
}

func (m model) viewFormScreen() string {
	form := m.viewForm()
	height := max(8, m.height-5)
	width := max(20, m.width)

	box := lipgloss.NewStyle().
		Width(width).
		Height(height).
		Align(lipgloss.Center, lipgloss.Center)

	hint := lipgloss.NewStyle().
		Foreground(lipgloss.Color("245")).
		Render("Connection editor")

	return box.Render(lipgloss.JoinVertical(lipgloss.Center, hint, "", form))
}

func testConnectionCmd(opts Options, name string) tea.Cmd {
	return func() tea.Msg {
		session, err := openSession(opts, name, defaultString(opts.Schema, "public"))
		if err != nil {
			return connTestMsg{name: name, err: err}
		}
		defer session.Conn.Close(context.Background())

		columns, rows, _, err := db.Query(context.Background(), session.Conn, db.ConnectionTestSQL())
		if err != nil {
			return connTestMsg{name: name, err: err}
		}
		return connTestMsg{name: name, text: renderTable(columns, rows)}
	}
}

func loadSchemaTablesCmd(opts Options, name string, schema string) tea.Cmd {
	return func() tea.Msg {
		session, err := openSession(opts, name, schema)
		if err != nil {
			return schemaTablesMsg{err: err}
		}
		defer session.Conn.Close(context.Background())

		sql := db.TablesSQL(false)
		args := []any(nil)
		if schema != "" {
			sql = db.TablesSQL(true)
			args = []any{schema}
		}

		_, rows, _, err := db.Query(context.Background(), session.Conn, sql, args...)
		if err != nil {
			return schemaTablesMsg{err: err}
		}

		tables := make([]string, 0, len(rows))
		for _, row := range rows {
			if len(row) > 1 {
				tables = append(tables, row[0]+"."+row[1])
			}
		}
		return schemaTablesMsg{tables: tables}
	}
}

func runSchemaCmd(opts Options, name string, schema string, title string, sql string, args ...any) tea.Cmd {
	return func() tea.Msg {
		session, err := openSession(opts, name, schema)
		if err != nil {
			return schemaMsg{title: title, err: err}
		}
		defer session.Conn.Close(context.Background())

		columns, rows, _, err := db.Query(context.Background(), session.Conn, sql, args...)
		if err != nil {
			return schemaMsg{title: title, err: err}
		}
		return schemaMsg{title: title, text: renderTable(columns, rows)}
	}
}

func runDataPreviewCmd(opts Options, name string, schema string, table string, limit int, sortColumn string, sortDesc bool, filters []db.PreviewFilter) tea.Cmd {
	return func() tea.Msg {
		session, err := openSession(opts, name, schema)
		if err != nil {
			return dataPageMsg{title: "Data " + table, schema: schema, table: table, pageSize: limit, err: err}
		}
		defer session.Conn.Close(context.Background())

		sql, args := db.BuildPreviewQuery(schema, table, limit, 0, sortColumn, sortDesc, filters)
		columns, rows, _, err := db.Query(context.Background(), session.Conn, sql, args...)
		if err != nil {
			return dataPageMsg{title: "Data " + table, schema: schema, table: table, pageSize: limit, err: err}
		}
		return dataPageMsg{
			title:    fmt.Sprintf("Data %s (%d rows loaded)", table, len(rows)),
			schema:   schema,
			table:    table,
			columns:  columns,
			rows:     rows,
			pageSize: limit,
		}
	}
}

func loadMoreDataCmd(opts Options, name string, schema string, table string, offset int, limit int, sortColumn string, sortDesc bool, filters []db.PreviewFilter) tea.Cmd {
	return func() tea.Msg {
		session, err := openSession(opts, name, schema)
		if err != nil {
			return dataPageMsg{title: "Data " + table, schema: schema, table: table, offset: offset, pageSize: limit, err: err}
		}
		defer session.Conn.Close(context.Background())

		sql, args := db.BuildPreviewQuery(schema, table, limit, offset, sortColumn, sortDesc, filters)
		columns, rows, _, err := db.Query(context.Background(), session.Conn, sql, args...)
		if err != nil {
			return dataPageMsg{title: "Data " + table, schema: schema, table: table, offset: offset, pageSize: limit, err: err}
		}
		return dataPageMsg{
			title:    fmt.Sprintf("Data %s (%d rows loaded)", table, offset+len(rows)),
			schema:   schema,
			table:    table,
			columns:  columns,
			rows:     rows,
			offset:   offset,
			pageSize: limit,
		}
	}
}

func runQueryCmd(opts Options, name string, schema string, sql string) tea.Cmd {
	return func() tea.Msg {
		session, err := openSession(opts, name, schema)
		if err != nil {
			return queryMsg{err: err}
		}
		defer session.Conn.Close(context.Background())

		columns, rows, affected, err := db.Query(context.Background(), session.Conn, sql)
		if err != nil {
			return queryMsg{err: err}
		}
		text := renderTable(columns, rows)
		if affected > 0 {
			text += fmt.Sprintf("\n%d rows affected\n", affected)
		}
		return queryMsg{text: text}
	}
}

func runExecCmd(opts Options, name string, schema string, path string, pattern string, vars map[string]string, dryRun bool, inTx bool, stopOnError bool) tea.Cmd {
	return func() tea.Msg {
		statements, err := sqlrun.Collect(path, pattern, vars)
		if err != nil {
			return execMsg{err: err}
		}

		if dryRun {
			rows := make([][]string, 0, len(statements))
			for i, stmt := range statements {
				rows = append(rows, []string{
					fmt.Sprint(i + 1),
					stmt.File,
					fmt.Sprint(stmt.StartLine),
					sqlrun.PreviewSQL(stmt.Text),
				})
			}
			return execMsg{text: renderTable([]string{"#", "file", "line", "statement"}, rows)}
		}

		session, err := openSession(opts, name, schema)
		if err != nil {
			return execMsg{err: err}
		}
		defer session.Conn.Close(context.Background())

		summary, err := sqlrun.Execute(context.Background(), session.Conn, statements, sqlrun.ExecuteOptions{
			Transaction: inTx,
			StopOnError: stopOnError,
			Pattern:     pattern,
		})
		if err != nil {
			return execMsg{err: err}
		}

		rows := make([][]string, 0, len(summary.Results))
		for i, result := range summary.Results {
			rows = append(rows, []string{
				fmt.Sprint(i + 1),
				result.File,
				result.Statement,
				result.Status,
				result.Rows,
				result.Time,
			})
		}
		text := renderTable([]string{"#", "file", "statement", "status", "rows", "time"}, rows)
		text += fmt.Sprintf("\nElapsed: %s\nFailures: %d\n", summary.Elapsed.Round(time.Millisecond), summary.Failures)
		return execMsg{text: text}
	}
}

func saveConnectionCmd(oldName string, newName string, conn config.Connection) tea.Cmd {
	return func() tea.Msg {
		cfg, err := config.Load()
		if err != nil {
			return formSaveMsg{err: err}
		}

		if oldName != "" && oldName != newName {
			delete(cfg.Connections, oldName)
			if cfg.Default == oldName {
				cfg.Default = newName
			}
		}

		cfg.Connections[newName] = conn
		if cfg.Default == "" {
			cfg.Default = newName
		}

		if err := config.Save(cfg); err != nil {
			return formSaveMsg{err: err}
		}

		return formSaveMsg{cfg: cfg, current: newName}
	}
}

func deleteConnectionCmd(name string) tea.Cmd {
	return func() tea.Msg {
		cfg, err := config.Load()
		if err != nil {
			return deleteConnMsg{err: err}
		}
		if _, ok := cfg.Connections[name]; !ok {
			return deleteConnMsg{err: fmt.Errorf("connection %q not found", name)}
		}

		delete(cfg.Connections, name)
		current := cfg.Default
		if cfg.Default == name {
			cfg.Default = ""
			for candidate := range cfg.Connections {
				cfg.Default = candidate
				current = candidate
				break
			}
		}
		if current == name {
			current = cfg.Default
		}

		if err := config.Save(cfg); err != nil {
			return deleteConnMsg{err: err}
		}

		return deleteConnMsg{cfg: cfg, current: current}
	}
}

func setDefaultCmd(name string) tea.Cmd {
	return func() tea.Msg {
		cfg, err := config.Load()
		if err != nil {
			return setDefaultMsg{err: err}
		}
		if _, ok := cfg.Connections[name]; !ok {
			return setDefaultMsg{err: fmt.Errorf("connection %q not found", name)}
		}
		cfg.Default = name
		if err := config.Save(cfg); err != nil {
			return setDefaultMsg{err: err}
		}
		return setDefaultMsg{cfg: cfg}
	}
}

func saveDataPresetCmd(tableKey string, preset config.DataPreset) tea.Cmd {
	return func() tea.Msg {
		cfg, err := config.Load()
		if err != nil {
			return dataPresetSaveMsg{err: err}
		}
		if cfg.DataPresets == nil {
			cfg.DataPresets = map[string][]config.DataPreset{}
		}

		presets := append([]config.DataPreset(nil), cfg.DataPresets[tableKey]...)
		replaced := false
		for i := range presets {
			if strings.EqualFold(presets[i].Name, preset.Name) {
				presets[i] = preset
				replaced = true
				break
			}
		}
		if !replaced {
			presets = append(presets, preset)
		}
		cfg.DataPresets[tableKey] = presets

		if err := config.Save(cfg); err != nil {
			return dataPresetSaveMsg{err: err}
		}
		return dataPresetSaveMsg{cfg: cfg, name: preset.Name}
	}
}

func deleteDataPresetCmd(tableKey string, name string) tea.Cmd {
	return func() tea.Msg {
		cfg, err := config.Load()
		if err != nil {
			return dataPresetDeleteMsg{err: err}
		}
		presets := append([]config.DataPreset(nil), cfg.DataPresets[tableKey]...)
		next := make([]config.DataPreset, 0, len(presets))
		removed := false
		for _, preset := range presets {
			if strings.EqualFold(preset.Name, name) {
				removed = true
				continue
			}
			next = append(next, preset)
		}
		if !removed {
			return dataPresetDeleteMsg{err: fmt.Errorf("preset %q not found", name)}
		}
		if len(next) == 0 {
			delete(cfg.DataPresets, tableKey)
		} else {
			cfg.DataPresets[tableKey] = next
		}
		if err := config.Save(cfg); err != nil {
			return dataPresetDeleteMsg{err: err}
		}
		return dataPresetDeleteMsg{cfg: cfg, name: name}
	}
}

func openSession(opts Options, name string, schema string) (*db.Session, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	return db.Open(ctx, cfg, db.Options{
		ConnectionName: name,
		URL:            opts.URL,
		Timeout:        opts.Timeout,
		Schema:         schema,
	})
}

func renderTable(columns []string, rows [][]string) string {
	var buf bytes.Buffer
	if err := output.Render(output.FormatTable, columns, rows, &buf); err != nil {
		return err.Error()
	}
	return buf.String()
}

func parseVarLines(input string) map[string]string {
	lines := strings.Split(input, "\n")
	vars := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		vars = append(vars, line)
	}
	return sqlrun.ParseVars(vars)
}

func parseInt(value string, fallback int) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	var number int
	_, err := fmt.Sscanf(value, "%d", &number)
	if err != nil {
		return fallback
	}
	return number
}

func parseTableAndLimit(value string, fallback int) (string, int) {
	parts := strings.Fields(strings.TrimSpace(value))
	if len(parts) == 0 {
		return "", fallback
	}
	if len(parts) == 1 {
		return parts[0], fallback
	}
	return parts[0], parseInt(parts[1], fallback)
}

func parseDataArgs(value string, fallback int) (int, string, []string) {
	parts := strings.Fields(strings.TrimSpace(value))
	limit := fallback
	filterParts := make([]string, 0, len(parts))
	excludeParts := make([]string, 0, len(parts))
	for _, part := range parts {
		if number, err := strconv.Atoi(part); err == nil {
			limit = number
			continue
		}
		if strings.HasPrefix(part, "-") && len(part) > 1 {
			excludeParts = append(excludeParts, strings.TrimPrefix(part, "-"))
			continue
		}
		filterParts = append(filterParts, part)
	}
	return limit, strings.Join(filterParts, " "), excludeParts
}

func splitQualifiedTable(value string) (string, string) {
	left, right, ok := strings.Cut(value, ".")
	if !ok {
		return "", value
	}
	return left, right
}

func (m *model) indexOfSchemaAction(target string) int {
	actions := m.visibleSchemaActions()
	for i, action := range actions {
		if action == target {
			return i
		}
	}
	return 0
}

func anyOrNone(value string) []any {
	if value == "" {
		return nil
	}
	return []any{value}
}

func intString(value int) string {
	if value == 0 {
		return ""
	}
	return fmt.Sprint(value)
}

func portValue(value int) string {
	if value == 0 {
		return "5432"
	}
	return fmt.Sprint(value)
}

func displayOr(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (m model) bodyHeight() int {
	return max(10, m.height-5)
}

func (m model) splitLayoutWidths(preferredLeft int, minRight int) (int, int, bool) {
	total := max(40, m.width-2)
	gap := 1
	minLeft := 20
	// Each pane uses width + 2 borders + 2 horizontal padding.
	chromePerPane := 4
	totalChrome := chromePerPane*2 + gap
	minimumCombined := minLeft + minRight + totalChrome

	if total < minimumCombined {
		full := max(20, total-chromePerPane)
		return full, full, true
	}

	left := preferredLeft
	right := total - left - totalChrome
	if right < minRight {
		right = minRight
		left = total - right - totalChrome
	}
	if left < minLeft {
		left = minLeft
		right = total - left - totalChrome
	}

	return left, max(minRight, right), false
}

func shorten(value string, width int) string {
	if width <= 4 {
		return value
	}
	runes := []rune(value)
	if len(runes) <= width {
		return value
	}
	return string(runes[:width-3]) + "..."
}

func clampBlock(text string, width int) string {
	if width <= 4 || text == "" {
		return text
	}
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		out = append(out, shorten(line, width))
	}
	return strings.Join(out, "\n")
}

func buildDataLine(rowLabel string, rowLabelWidth int, cells []string) string {
	_ = rowLabelWidth
	parts := make([]string, 0, len(cells)+1)
	parts = append(parts, rowLabel)
	parts = append(parts, cells...)
	return strings.Join(parts, " | ")
}

func buildDataSeparator(rowLabelWidth int, widths []int) string {
	parts := make([]string, 0, len(widths)+1)
	parts = append(parts, strings.Repeat("-", rowLabelWidth))
	for _, width := range widths {
		parts = append(parts, strings.Repeat("-", width))
	}
	return strings.Join(parts, "-+-")
}

func containsInt(values []int, target int) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func indexOfStringFold(values []string, target string) int {
	if target == "" {
		return -1
	}
	for i, value := range values {
		if strings.EqualFold(value, target) {
			return i
		}
	}
	return -1
}

func padLeft(value string, width int) string {
	padding := width - runeLen(value)
	if padding <= 0 {
		return value
	}
	return strings.Repeat(" ", padding) + value
}

func padRight(value string, width int) string {
	padding := width - runeLen(value)
	if padding <= 0 {
		return value
	}
	return value + strings.Repeat(" ", padding)
}

func runeLen(value string) int {
	return len([]rune(value))
}

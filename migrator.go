package migrator

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/anyufly/migrate-sql-result"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const defaultTimeFormat = "20060102150405"

var (
	errInvalidSequenceWidth     = errors.New("digits must be positive")
	errIncompatibleSeqAndFormat = errors.New("the seq and format options are mutually exclusive")
)

type migrateFunc func() (*result.MigrateSQLResult, error)

type Migrator struct {
	migrate            *migrate.Migrate
	migrationsFilePath string
	migrateFunc        migrateFunc
	logger             Logger
}

func New(driver database.Driver, databaseName, migrationsFilePath string, migrateFunc migrateFunc) (*Migrator, error) {
	m, err := migrate.NewWithDatabaseInstance(
		fmt.Sprintf("file://%s", migrationsFilePath),
		databaseName,
		driver,
	)

	if err != nil {
		return nil, err
	}

	return &Migrator{
		migrate:            m,
		migrationsFilePath: migrationsFilePath,
		migrateFunc:        migrateFunc,
		logger:             defaultLogger,
	}, nil
}

func nextSeqVersion(migrationsFilePath, ext string, seqDigits int) (string, error) {
	matches, err := filepath.Glob(filepath.Join(migrationsFilePath, "*"+ext))

	if err != nil {
		return "", err
	}

	if seqDigits <= 0 {
		return "", errInvalidSequenceWidth
	}

	nextSeq := uint64(1)

	if len(matches) > 0 {
		filename := matches[len(matches)-1]
		matchSeqStr := filepath.Base(filename)
		idx := strings.Index(matchSeqStr, "_")

		if idx < 1 { // Using 1 instead of 0 since there should be at least 1 digit
			return "", fmt.Errorf("malformed migration filename: %s", filename)
		}

		var err error
		matchSeqStr = matchSeqStr[0:idx]
		nextSeq, err = strconv.ParseUint(matchSeqStr, 10, 64)

		if err != nil {
			return "", err
		}

		nextSeq++
	}

	version := fmt.Sprintf("%0[2]*[1]d", nextSeq, seqDigits)

	if len(version) > seqDigits {
		return "", fmt.Errorf("next sequence number %s too large. At most %d digits are allowed", version, seqDigits)
	}

	return version, nil
}

func timeVersion(timeZoneName string, format string) (version string, err error) {
	var location *time.Location
	if timeZoneName == "" {
		location = time.Local
	} else {
		location, err = time.LoadLocation(timeZoneName)
		if err != nil {
			return "", err
		}
	}
	now := time.Now().In(location)

	switch format {
	case "":
		version = now.Format(defaultTimeFormat)
	case "unix":
		version = strconv.FormatInt(now.Unix(), 10)
	case "unixNano":
		version = strconv.FormatInt(now.UnixNano(), 10)
	default:
		version = now.Format(format)
	}

	return
}

func (m *Migrator) SetLogger(logger Logger) {
	m.migrate.Log = logger
	m.logger = logger
}

func (m *Migrator) upAndDownFilePath(
	timeZoneName string, format string, name string, ext string, seq bool, seqDigits int) (string, string, error) {

	if seq && format != defaultTimeFormat {
		return "", "", errIncompatibleSeqAndFormat
	}

	var version string
	var err error

	if ext == "" {
		ext = ".sql"
	} else {
		ext = "." + strings.TrimPrefix(ext, ".")
	}

	if seq {
		version, err = nextSeqVersion(m.migrationsFilePath, ext, seqDigits)

		if err != nil {
			return "", "", err
		}
	} else {
		version, err = timeVersion(timeZoneName, format)

		if err != nil {
			return "", "", err
		}
	}

	versionGlob := filepath.Join(m.migrationsFilePath, version+"_*"+ext)
	matches, err := filepath.Glob(versionGlob)

	if err != nil {
		return "", "", err
	}

	if len(matches) > 0 {
		return "", "", fmt.Errorf("duplicate migration version: %s", version)
	}

	up := filepath.Join(m.migrationsFilePath, fmt.Sprintf("%s_%s.%s%s", version, name, "up", ext))
	down := filepath.Join(m.migrationsFilePath, fmt.Sprintf("%s_%s.%s%s", version, name, "up", ext))

	return up, down, nil
}

func (m *Migrator) MakeMigrate(timeZoneName string, format string, name string, ext string, seq bool, seqDigits int) error {
	migrateResult, err := m.migrateFunc()

	if err != nil {
		return err
	}

	if migrateResult.Empty() {
		m.migrate.Log.Printf("no change")
		return nil
	}

	up, down, err := m.upAndDownFilePath(timeZoneName, format, name, ext, seq, seqDigits)

	if err != nil {
		return err
	}

	var upBuffer, downBuffer bytes.Buffer

	for tableName, sqlList := range migrateResult.Up() {
		upBuffer.WriteString(fmt.Sprintf("--%s\n", tableName))

		for _, sql := range sqlList {
			upBuffer.WriteString(fmt.Sprintf("%s;\n", sql))
		}

	}

	for tableName, sqlList := range migrateResult.Down() {
		downBuffer.WriteString(fmt.Sprintf("--%s\n", tableName))

		for _, sql := range sqlList {
			downBuffer.WriteString(fmt.Sprintf("%s;\n", sql))
		}
	}

	err = os.WriteFile(up, upBuffer.Bytes(), 0666)
	if err != nil {
		return err
	}

	err = os.WriteFile(down, downBuffer.Bytes(), 0666)
	if err != nil {
		return err
	}
	return nil
}

func (m *Migrator) Up(n int) error {
	if n <= 0 {
		return m.migrate.Up()
	}
	return m.migrate.Steps(n)
}

func (m *Migrator) Down(n int) error {
	if n <= 0 {
		return m.migrate.Down()
	}
	return m.migrate.Steps(-n)
}

func (m *Migrator) Drop() error {
	return m.migrate.Drop()
}

func (m *Migrator) Force(version int) error {
	return m.migrate.Force(version)
}

func (m *Migrator) Goto(version uint) error {
	return m.migrate.Migrate(version)
}

func (m *Migrator) Version() (version uint, dirty bool, err error) {
	return m.migrate.Version()
}

func (m *Migrator) CobraCommand() *cobra.Command {
	return (&migratorCobraCommandBuilder{migrator: m}).Build()
}

func (m *Migrator) Close() (source error, database error) {
	return m.migrate.Close()
}

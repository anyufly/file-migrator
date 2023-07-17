package migrator

import (
	"fmt"
	"github.com/anyufly/logger/loggers"
	"github.com/golang-migrate/migrate/v4"
)

type Logger interface {
	migrate.Logger
	SetVerbose(bool)
	Info(msg string, keyAndValues ...interface{})
	Error(msg string, keyAndValues ...interface{})
	Fatal(msg string, keyAndValues ...interface{})
}

var defaultLogger = &migrateLogger{
	logger: loggers.Logger.Name("migrator"),
}

type migrateLogger struct {
	logger  *loggers.CommonLogger
	verbose bool
}

func (m *migrateLogger) Printf(format string, v ...interface{}) {
	m.logger.Info(fmt.Sprintf(format, v))
}

func (m *migrateLogger) Verbose() bool {
	return m.verbose
}

func (m *migrateLogger) SetVerbose(b bool) {
	m.verbose = b
}

func (m *migrateLogger) Error(msg string, keyAndValues ...interface{}) {
	m.logger.Sugar().Errorw(msg, keyAndValues...)
}

func (m *migrateLogger) Fatal(msg string, keyAndValues ...interface{}) {
	m.logger.Sugar().Fatalw(msg, keyAndValues...)
}

func (m *migrateLogger) Info(msg string, keyAndValues ...interface{}) {
	m.logger.Sugar().Infow(msg, keyAndValues...)
}

package migrator

import (
	"errors"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	migrateUsage     = "migrate OPTIONS COMMAND [arg...]"
	migrateUsageDesc = `a CLI command for migrate databases`

	createUsage     = "create [-ext E] [-seq] [-digits N] [-format] [-tz] NAME"
	createUsageDesc = `Create a set of timestamped up/down migrations titled NAME, with extension E.
	Use -seq option to generate sequential up/down migrations with N digits.
	Use -format option to specify a Go time format string. Note: migrations with the same time cause "duplicate migration version" error.
	Use -tz option to specify the timezone that will be used when generating non-sequential migrations (defaults: Local).`
	gotoUsage     = "goto V"
	gotoUsageDesc = `Migrate to version V`

	upUsage     = "up [N]"
	upUsageDesc = "Apply all or N up migrations"

	downUsage     = "down [N] [-all]"
	downUsageDesc = `Apply all or N down migrations
	Use -all to apply all down migrations`

	dropUsage     = "drop [-f]"
	dropUsageDesc = `Drop everything inside database
	Use -f to bypass confirmation`

	forceUsage     = "force V"
	forceUsageDesc = `Set version V but don't run migration (ignores dirty state)`

	versionUsage     = "version"
	versionUsageDesc = "Print current migration version"
)

type migrateFlag struct {
	verbosePtr     *bool
	prefetchPtr    *uint
	lockTimeoutPtr *uint
}

type createFlag struct {
	extPtr       *string
	seqPtr       *bool
	seqDigitsPtr *int
	formatPtr    *string
	tzPtr        *string
}

type downFlag struct {
	allPtr *bool
}

type dropFlag struct {
	forceDropPtr *bool
}

type migratorCobraCommandBuilder struct {
	migrator *Migrator
	*migrateFlag
	*createFlag
	*downFlag
	*dropFlag
}

func (builder *migratorCobraCommandBuilder) Build() *cobra.Command {
	return builder.buildMigrateCmd()
}

func (builder *migratorCobraCommandBuilder) buildMigrateCmd() *cobra.Command {
	migrateCommand := &cobra.Command{
		Use:   migrateUsage,
		Short: migrateUsageDesc,
		Long:  migrateUsageDesc,
	}

	migrateCommand.PersistentFlags().BoolVar(builder.verbosePtr, "verbose", false, "Print verbose logging")
	migrateCommand.PersistentFlags().UintVar(builder.prefetchPtr, "prefetch", 10, "Number of migrations to load in advance before executing (default 10)")
	migrateCommand.PersistentFlags().UintVar(builder.lockTimeoutPtr, "lock-timeout", 15, "Allow N seconds to acquire database lock (default 15)")

	createCommand := builder.buildCreateCmd()
	migrateCommand.AddCommand(createCommand)

	gotoCommand := builder.buildGotoCmd()
	migrateCommand.AddCommand(gotoCommand)

	upCommand := builder.buildUpCommand()
	migrateCommand.AddCommand(upCommand)

	downCommand := builder.buildDownCommand()
	migrateCommand.AddCommand(downCommand)

	dropCommand := builder.buildDropCommand()
	migrateCommand.AddCommand(dropCommand)

	forceCommand := builder.buildForceCommand()
	migrateCommand.AddCommand(forceCommand)

	versionCommand := builder.buildVersionCommand()
	migrateCommand.AddCommand(versionCommand)

	return migrateCommand

}

func (builder *migratorCobraCommandBuilder) setupMigrator() {
	if verbose := *builder.verbosePtr; verbose {
		builder.migrator.logger.SetVerbose(verbose)
	}

	builder.migrator.migrate.PrefetchMigrations = *builder.prefetchPtr
	builder.migrator.migrate.LockTimeout = time.Duration(*builder.lockTimeoutPtr) * time.Second

	// handle Ctrl+c
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT)
	go func() {
		for range signals {
			builder.migrator.logger.Info("Stopping after this running migration ...")
			builder.migrator.migrate.GracefulStop <- true
			return
		}
	}()
}

func (builder *migratorCobraCommandBuilder) closeMigrator() {
	sourceErr, databaseErr := builder.migrator.Close()
	if sourceErr != nil || databaseErr != nil {
		builder.migrator.logger.Error("encountered an error when close migrator", "sourceErr", sourceErr, "databaseErr", databaseErr)
	}
}

func (builder *migratorCobraCommandBuilder) buildCreateCmd() *cobra.Command {
	createCommand := &cobra.Command{
		Use:   createUsage,
		Short: createUsageDesc,
		Long:  createUsageDesc,
		Run: func(cmd *cobra.Command, args []string) {
			defer builder.closeMigrator()
			builder.setupMigrator()

			if len(args) == 0 {
				builder.migrator.logger.Fatal("please specify name")
			}
			name := args[0]

			err := builder.migrator.MakeMigrate(
				*builder.tzPtr,
				*builder.formatPtr,
				name,
				*builder.extPtr,
				*builder.seqPtr,
				*builder.seqDigitsPtr)

			if err != nil {
				builder.migrator.logger.Fatal(err.Error())
			}

		},
	}

	createCommand.Flags().StringVar(builder.extPtr, "ext", "", "File extension")
	createCommand.Flags().BoolVar(builder.seqPtr, "seq", false, "Use sequential numbers instead of timestamps (default: false)")
	createCommand.Flags().IntVar(builder.seqDigitsPtr, "digits", 6, "The number of digits to use in sequences (default: 6)")
	createCommand.Flags().StringVar(builder.formatPtr, "format", "", `The Go time format string to use. If the string "unix" or "unixNano" is specified, then the seconds or nanoseconds since January 1, 1970 UTC respectively will be used. Caution, due to the behavior of time.Time.Format(), invalid format strings will not error`)
	createCommand.Flags().StringVar(builder.tzPtr, "tz", "", `The timezone that will be used for format time (default: local)`)

	return createCommand

}

func (builder *migratorCobraCommandBuilder) buildGotoCmd() *cobra.Command {
	gotoCommand := &cobra.Command{
		Use:   gotoUsage,
		Short: gotoUsageDesc,
		Long:  gotoUsageDesc,
		Run: func(cmd *cobra.Command, args []string) {
			defer builder.closeMigrator()
			builder.setupMigrator()

			if len(args) == 0 {
				builder.migrator.logger.Fatal("please specify version argument V")
			}

			v, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				builder.migrator.logger.Fatal("can't read version argument V")
			}

			startTime := time.Now()

			if err = builder.migrator.Goto(uint(v)); err != nil {
				if err != migrate.ErrNoChange {
					builder.migrator.logger.Fatal(err.Error())
				}
				builder.migrator.logger.Info(err.Error())
			}

			if *builder.verbosePtr {
				builder.migrator.logger.Info(fmt.Sprintf("Finished After %d ms", time.Since(startTime).Microseconds()))
			}
		},
	}

	return gotoCommand
}

func (builder *migratorCobraCommandBuilder) buildUpCommand() *cobra.Command {
	upCommand := &cobra.Command{
		Use:   upUsage,
		Short: upUsageDesc,
		Long:  upUsageDesc,
		Run: func(cmd *cobra.Command, args []string) {
			defer builder.closeMigrator()
			builder.setupMigrator()

			limit := -1

			if len(args) > 0 {
				n, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					builder.migrator.logger.Fatal("can't read limit argument N", "error", err)
				}
				limit = int(n)
			}

			startTime := time.Now()
			if err := builder.migrator.Up(limit); err != nil {
				if err != migrate.ErrNoChange {
					builder.migrator.logger.Fatal(err.Error())
				}
				builder.migrator.logger.Info(err.Error())
			}

			if *builder.verbosePtr {
				builder.migrator.logger.Info(fmt.Sprintf("Finished After %d ms", time.Since(startTime).Microseconds()))
			}

		},
	}

	return upCommand
}

func numDownMigrationsFromArgs(applyAll bool, args []string) (int, bool, error) {
	if applyAll {
		if len(args) > 0 {
			return 0, false, errors.New("-all cannot be used with other arguments")
		}
		return -1, false, nil
	}

	switch len(args) {
	case 0:
		return -1, true, nil
	case 1:
		downValue := args[0]
		n, err := strconv.ParseUint(downValue, 10, 64)
		if err != nil {
			return 0, false, errors.New("can't read limit argument N")
		}
		return int(n), false, nil
	default:
		return 0, false, errors.New("too many arguments")
	}
}

func (builder *migratorCobraCommandBuilder) buildDownCommand() *cobra.Command {
	downCommand := &cobra.Command{
		Use:   downUsage,
		Short: downUsageDesc,
		Long:  downUsageDesc,
		Run: func(cmd *cobra.Command, args []string) {
			defer builder.closeMigrator()
			builder.setupMigrator()

			num, needsConfirm, err := numDownMigrationsFromArgs(*builder.allPtr, args)
			if err != nil {
				builder.migrator.logger.Fatal(err.Error())
			}

			if needsConfirm {
				fmt.Println("Are you sure you want to apply all down migrations? [y/N]")
				var response string
				_, _ = fmt.Scanln(&response)

				response = strings.ToLower(strings.TrimSpace(response))

				if response == "y" {
					builder.migrator.logger.Info("Applying all down migrations")
				} else {
					builder.migrator.logger.Fatal("Not applying all down migrations")
				}
			}

			startTime := time.Now()
			if err = builder.migrator.Down(num); err != nil {
				if err != migrate.ErrNoChange {
					builder.migrator.logger.Fatal(err.Error())
				}
				builder.migrator.logger.Info(err.Error())
			}

			if *builder.verbosePtr {
				builder.migrator.logger.Info(fmt.Sprintf("Finished After %d ms", time.Since(startTime).Microseconds()))
			}

		},
	}

	downCommand.Flags().BoolVar(builder.allPtr, "all", false, "Apply all down migrations")

	return downCommand
}

func (builder *migratorCobraCommandBuilder) buildDropCommand() *cobra.Command {
	dropCommand := &cobra.Command{
		Use:   dropUsage,
		Short: dropUsageDesc,
		Long:  dropUsageDesc,
		Run: func(cmd *cobra.Command, args []string) {
			defer builder.closeMigrator()
			builder.setupMigrator()

			if !*builder.forceDropPtr {
				fmt.Println("Are you sure you want to drop the entire database schema? [y/N]")
				var response string
				_, _ = fmt.Scanln(&response)
				response = strings.ToLower(strings.TrimSpace(response))

				if response == "y" {
					builder.migrator.logger.Info("Dropping the entire database schema")
				} else {
					builder.migrator.logger.Fatal("Aborted dropping the entire database schema")
				}
			}

			startTime := time.Now()
			if err := builder.migrator.Drop(); err != nil {
				builder.migrator.logger.Fatal(err.Error())
			}

			if *builder.verbosePtr {
				builder.migrator.logger.Info(fmt.Sprintf("Finished After %d ms", time.Since(startTime).Microseconds()))
			}
		},
	}

	dropCommand.Flags().BoolVar(builder.forceDropPtr, "f", false, "Force the drop command by bypassing the confirmation prompt")

	return dropCommand
}

func (builder *migratorCobraCommandBuilder) buildForceCommand() *cobra.Command {
	forceCommand := &cobra.Command{
		Use:   forceUsage,
		Short: forceUsageDesc,
		Long:  forceUsageDesc,
		Run: func(cmd *cobra.Command, args []string) {
			defer builder.closeMigrator()
			builder.setupMigrator()

			if len(args) == 0 {
				builder.migrator.logger.Fatal("please specify version argument V")
			}

			v, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				builder.migrator.logger.Fatal("can't read version argument V")
			}

			if v < -1 {
				builder.migrator.logger.Fatal("argument V must be >= -1")
			}

			startTime := time.Now()
			if err = builder.migrator.Force(int(v)); err != nil {
				builder.migrator.logger.Fatal(err.Error())
			}

			if *builder.verbosePtr {
				builder.migrator.logger.Info(fmt.Sprintf("Finished After %d ms", time.Since(startTime).Microseconds()))
			}

		},
	}

	return forceCommand
}

func (builder *migratorCobraCommandBuilder) buildVersionCommand() *cobra.Command {
	versionCommand := &cobra.Command{
		Use:   versionUsage,
		Short: versionUsageDesc,
		Long:  versionUsageDesc,
		Run: func(cmd *cobra.Command, args []string) {
			defer builder.closeMigrator()
			builder.setupMigrator()

			if version, dirty, err := builder.migrator.Version(); err != nil {
				builder.migrator.logger.Fatal(err.Error())
			} else {
				if dirty {
					builder.migrator.logger.Printf("%v (dirty)\n", version)
				} else {
					builder.migrator.logger.Printf("%v", version)
				}
			}
		},
	}

	return versionCommand
}

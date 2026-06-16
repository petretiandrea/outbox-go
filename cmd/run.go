package main

import (
	"fmt"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/petretiandrea/outbox-go/internal/bootstrap"
	"github.com/petretiandrea/outbox-go/internal/config"
)

func newRunCommand() *cobra.Command {
	var configPath string
	var envPrefix string

	command := &cobra.Command{
		Use:   "run",
		Short: "Start the outbox forwarder service",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(config.LoadOptions{
				FilePath:  configPath,
				EnvPrefix: envPrefix,
			})
			if err != nil {
				return err
			}

			runtime, err := bootstrap.NewRuntimeWithFactories(
				cfg,
				bootstrap.NewDefaultSourceFactory(),
				bootstrap.NewDefaultPublisherFactory(),
			)
			if err != nil {
				return err
			}
			defer runtime.Close()

			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			fmt.Fprintf(cmd.OutOrStdout(), "starting outbox forwarder with %d channel publisher(s)\n", len(cfg.Channels))

			return runtime.Run(ctx)
		},
	}

	command.Flags().StringVar(&configPath, "config", "", "path to the service config file")
	command.Flags().StringVar(&envPrefix, "env-prefix", "", "environment variable prefix override")

	return command
}

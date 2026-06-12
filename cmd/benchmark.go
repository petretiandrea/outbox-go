package main

import "github.com/spf13/cobra"

func newBenchmarkCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "benchmark",
		Short: "Run outbox benchmarks",
	}

	command.AddCommand(newPostgresBenchmarkCommand())

	return command
}

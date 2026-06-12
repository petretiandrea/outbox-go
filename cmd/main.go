package main

import (
	"log"

	"github.com/spf13/cobra"
)

func main() {
	command := &cobra.Command{
		Use:   "outbox",
		Short: "Run the outbox forwarder service",
	}

	command.AddCommand(newRunCommand())
	command.AddCommand(newBenchmarkCommand())

	if err := command.Execute(); err != nil {
		log.Fatal(err)
	}
}

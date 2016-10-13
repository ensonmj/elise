package app

import "github.com/spf13/cobra"

var WebCmd = &cobra.Command{
	Use:   "web",
	Short: "Demonstrate parse result via http",
	RunE: func(cmd *cobra.Command, args []string) error {
		return web()
	},
}

func web() error {
	return nil
}

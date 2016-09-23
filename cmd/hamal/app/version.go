package app

import (
	"fmt"

	"github.com/spf13/cobra"
)

const gVersion = "0.0.1"

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version.",
	Long:  "The Version of the hamal application.",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(gVersion)
	},
}

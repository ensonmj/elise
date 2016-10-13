package app

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var WebCmd = &cobra.Command{
	Use:   "web",
	Short: "Demonstrate parse result via http",
	RunE: func(cmd *cobra.Command, args []string) error {
		return web()
	},
}

var (
	fPort string
)

func init() {
	flags := WebCmd.Flags()
	flags.StringVarP(&fPort, "port", "p", "8080", "the server port")
	viper.BindPFlag("port", flags.Lookup("port"))
}

func web() error {
	fs := http.FileServer(http.Dir(fPubDir))
	http.Handle("/", fs)
	addr := ":" + fPort
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		return err
	}
	return nil
}

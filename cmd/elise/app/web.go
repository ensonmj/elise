package app

import (
	"net/http"

	"github.com/ensonmj/elise/cmd/elise/assets"
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
	fPort       string
	fWebDevMode bool
)

func init() {
	flags := WebCmd.Flags()
	flags.StringVarP(&fPort, "port", "p", "8080", "the server port")
	flags.BoolVarP(&fWebDevMode, "devMode", "D", false, "develop mode, using local assets")
	viper.BindPFlag("port", flags.Lookup("port"))
	viper.BindPFlag("devMode", flags.Lookup("devMode"))
}

func web() error {
	http.Handle("/", http.FileServer(http.Dir(fPubDir)))
	http.Handle("/assets/", http.FileServer(assets.FS(fWebDevMode)))

	addr := ":" + fPort
	return http.ListenAndServe(addr, nil)
}

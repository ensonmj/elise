package app

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/ensonmj/elise/cmd/elise/assets"
	"github.com/spf13/cobra"
)

var (
	fWebPort    string
	fWebDevMode bool
	fWebPubDir  string
)

func init() {
	flags := WebCmd.Flags()
	flags.StringVarP(&fWebPort, "port", "p", "8080", "the server port")
	flags.BoolVarP(&fWebDevMode, "devMode", "D", false, "develop mode, using local assets")
	flags.StringVarP(&fWebPubDir, "pubDir", "P", "./pub", "public dir for store demonstration file")
}

var WebCmd = &cobra.Command{
	Use:   "web",
	Short: "Demonstrate parse result via http.",
	RunE: func(cmd *cobra.Command, args []string) error {
		return web()
	},
}

func web() error {
	http.Handle("/", http.FileServer(http.Dir(fWebPubDir)))
	http.Handle("/assets/", http.FileServer(assets.FS(fWebDevMode)))
	http.Handle("/proxy", newImgProxy())

	addr := ":" + fWebPort
	return http.ListenAndServe(addr, nil)
}

func newImgProxy() *httputil.ReverseProxy {
	director := func(req *http.Request) {
		path := strings.TrimPrefix(req.RequestURI, "/proxy?target=")
		remote, _ := url.Parse(path)
		req.URL = remote
		req.Host = remote.Host
		// some server reject by response 403 when referer illegal, just delete it
		req.Header.Del("Referer")
	}
	return &httputil.ReverseProxy{Director: director}
}

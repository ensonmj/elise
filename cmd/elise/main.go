package main

//go:generate esc -pkg=assets -ignore=(swp|go)$DOLLAR -o=assets/assets.go assets
//go:generate esc -pkg=conf -ignore=(swp|go)$DOLLAR -o=conf/conf.go conf

import (
	"net/http"
	_ "net/http/pprof"

	"github.com/ensonmj/elise/cmd/elise/app"
	"github.com/spf13/viper"
)

func main() {
	// /debug/pprof for profile
	go func() {
		http.ListenAndServe("127.0.0.1:5196", nil)
	}()

	viper.SetEnvPrefix("ELISE")
	viper.AutomaticEnv()

	app.EliseCmd.Execute()
}

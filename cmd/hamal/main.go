package main

import (
	"net/http"
	_ "net/http/pprof"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/ensonmj/hamal/cmd/hamal/app"
	"github.com/spf13/viper"
)

func main() {
	// /debug/pprof for profile
	go func() {
		http.ListenAndServe("127.0.0.1:5196", nil)
	}()

	viper.SetEnvPrefix("HAMAL")
	viper.AutomaticEnv()
	viper.AddConfigPath("./conf")
	viper.SetConfigName("config")
	//viper.WatchConfig() // watching and re-reading config file
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}

	if err := app.HamalCmd.Execute(); err != nil {
		log.WithError(err).Fatal("hamal exit")
		os.Exit(-1)
	}
}

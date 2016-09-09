package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/sclevine/agouti"
	"github.com/spf13/viper"
)

func main() {
	log.SetLevel(log.DebugLevel)

	viper.AddConfigPath("./conf")
	viper.SetConfigName("config")
	viper.WatchConfig() // watching and re-reading config file
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}

	driver := agouti.PhantomJS()
	if err := driver.Start(); err != nil {
		log.Fatalf("failed to start driver:%v", err)
	}
	defer driver.Stop()

	page, err := driver.NewPage(agouti.Browser("phantomjs"))
	if err != nil {
		log.Fatalf("failed to open page:%v", err)
	}

	jsActions := make(map[string]struct {
		Funcs   []string
		ResFile *os.File
	})
	defer func() {
		for _, v := range jsActions {
			v.ResFile.Close()
		}
	}()

	filepath.Walk("./data", func(path string, f os.FileInfo, err error) error {
		if f.IsDir() {
			return nil
		}
		log.WithFields(log.Fields{
			"path":     path,
			"fileName": f.Name(),
		}).Debug("Crawler input file")

		// find script
		filename := f.Name()
		if !viper.IsSet(filename) {
			log.WithFields(log.Fields{
				"filename": filename,
			}).Info("No conf item for file")
			return nil
		}
		conf := viper.GetStringMap(filename)
		log.WithFields(log.Fields{
			"conf": conf,
		}).Debug("Read conf")
		if val, ok := conf["enable"]; ok && !val.(bool) {
			log.WithFields(log.Fields{
				"conf": conf,
			}).Info("Dissabled")
			return nil
		}

		// load script
		jsAction, ok := jsActions[filename]
		if !ok {
			res_path := "./output/" + conf["output_file"].(string)
			file, err := os.Create(res_path)
			if err != nil {
				log.WithFields(log.Fields{
					"res_path": res_path,
					"err":      err,
				}).Info("Failed to create output file")
				return nil
			}

			// script_path := "./script/" + conf["script_name"]
			var script_path []string
			switch scripts := conf["script_name"].(type) {
			case string:
				script_path = append(script_path, "./script/"+scripts)
			case []interface{}:
				for _, v := range scripts {
					script_path = append(script_path, "./script/"+v.(string))
				}
			default:
				log.WithFields(log.Fields{
					"script_path": scripts,
				}).Warn("Type err")
				return nil
			}

			for _, v := range script_path {
				data, err := ioutil.ReadFile(v)
				if err != nil {
					log.WithFields(log.Fields{
						"script_path": v,
						"err":         err,
					}).Info("Failed to read script")

					return nil
				}

				jsAction.Funcs = append(jsAction.Funcs, string(data))
				jsAction.ResFile = file
				jsActions[filename] = jsAction
			}
			log.WithFields(log.Fields{
				"jsAction": jsAction,
			}).Info("Create js action")
		}
		log.WithFields(log.Fields{
			"conf": conf,
		}).Debug("Found conf")

		file, err := os.Open(path)
		if err != nil {
			log.WithFields(log.Fields{
				"path": path,
				"err":  err,
			}).Info("Failed to open data file")

			return nil
		}
		defer file.Close()

		sc := bufio.NewScanner(file)
	CRAWLOOP:
		for sc.Scan() {
			url := sc.Text()
			log.WithFields(log.Fields{
				"url": url,
			}).Debug("Crawl url")

			if err := page.Navigate(url); err != nil {
				log.WithFields(log.Fields{
					"url": url,
					"err": err,
				}).Warn("Failed to navigate url")
				continue
			}

			// res := make(map[string]interface{})
			var res map[string]interface{}
			for i, jsFunc := range jsAction.Funcs {
				err := page.RunScript(jsFunc, res, &res)
				log.WithFields(log.Fields{
					"i":   i,
					"res": res,
					"err": err,
				}).Debug("Crawl response")
				if err != nil {
					continue CRAWLOOP
				}
				if val, ok := res["stop"]; ok && val.(bool) {
					continue CRAWLOOP
				}
				if val, ok := res["waitTime"]; ok {
					delete(res, "stop")
					delete(res, "waitTime")
					d := time.Duration(val.(float64)) * time.Millisecond
					time.Sleep(d)
				}
			}

			data, _ := json.Marshal(res)
			jsAction.ResFile.WriteString(fmt.Sprintf("%s\t%s\n", url, string(data)))
		}

		return nil
	})
}

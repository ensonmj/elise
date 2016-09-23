package app

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/rifflock/lfshook"
	"github.com/sclevine/agouti"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	HamalCmd.AddCommand(versionCmd)

	pflags := HamalCmd.PersistentFlags()
	pflags.IntVarP(&fVerbose, "verbose", "v", 4, "log level: 0~5, 5 for debug detail")
	viper.BindPFlag("verbose", pflags.Lookup("verbose"))

	flags := HamalCmd.Flags()
	flags.IntVarP(&fParallel, "parallel", "p", 10, "max number of parallel exector")
	flags.StringVar(&fDataDir, "dataDir", "./data", "dir for storage url files")
	flags.StringVar(&fScriptDir, "scriptDir", "./script", "dir for storage scripts")
	flags.StringVar(&fOutputDir, "outputDir", "./output", "dir for storage parse result files")
	flags.StringVar(&fLogDir, "logDir", "./log", "dir for storage log")
	viper.BindPFlag("parallel", flags.Lookup("parallel"))
	viper.BindPFlag("dataDir", flags.Lookup("dataDir"))
	viper.BindPFlag("scriptDir", flags.Lookup("scriptDir"))
	viper.BindPFlag("outputDir", flags.Lookup("outputDir"))
	viper.BindPFlag("logDir", flags.Lookup("logDir"))
}

type URLInfo struct {
	URL     string
	Ofile   *os.File
	JsFuncs []string
}

var (
	fParallel  int
	fVerbose   int
	fDataDir   string
	fScriptDir string
	fOutputDir string
	fLogDir    string
)

var HamalCmd = &cobra.Command{
	Use:   "hamal",
	Short: "Hamal parse webpage based on scripts.",
	RunE: func(cmd *cobra.Command, args []string) error {
		log.SetLevel(log.Level(fVerbose))

		if _, err := os.Stat(fLogDir); os.IsNotExist(err) {
			os.Mkdir(fLogDir, os.ModePerm)
		}
		log.AddHook(lfshook.NewHook(lfshook.PathMap{
			log.DebugLevel: fLogDir + "/debug.log",
			log.InfoLevel:  fLogDir + "/info.log",
			log.WarnLevel:  fLogDir + "/warn.log",
			log.FatalLevel: fLogDir + "/fatal.log",
		}))

		if _, err := os.Stat(fDataDir); os.IsNotExist(err) {
			log.WithFields(log.Fields{
				"dataDir": fDataDir,
			}).Fatal("No script dir")
			panic(err)
		}
		if _, err := os.Stat(fScriptDir); os.IsNotExist(err) {
			log.WithFields(log.Fields{
				"scriptDir": fScriptDir,
			}).Fatal("No script dir")
			panic(err)
		}
		if _, err := os.Stat(fOutputDir); os.IsNotExist(err) {
			os.Mkdir(fOutputDir, os.ModePerm)
			log.WithFields(log.Fields{
				"outputDir": fOutputDir,
			}).Debug("Create output dir")
		}

		return mainFunc()
	},
}

func mainFunc() error {
	done := new(sync.WaitGroup)
	infoChan := make(chan URLInfo, fParallel)
	for i := 0; i < fParallel; i++ {
		go func(index int) {
		RESTART:
			driver := agouti.PhantomJS()
			if err := driver.Start(); err != nil {
				log.WithFields(log.Fields{
					"index": index,
				}).Fatalf("Failed to start driver:%v", err)
			}

			for {
				select {
				case info, ok := <-infoChan:
					if !ok {
						log.WithFields(log.Fields{
							"index": index,
						}).Debug("Worker exit")
						driver.Stop()
						return
					}
					// TODO: collect failed url and retry later
					err := parseURL(index, info, driver, done)
					if err != nil {
						// sometimes phantomjs crashed or timeout error
						// we can't differentiate cause of error
						driver.Stop()
						goto RESTART
					}
				}
			}
		}(i)
	}

	filepath.Walk(fDataDir, walkFile(infoChan, done))

	done.Wait()
	close(infoChan)

	return nil
}

func parseURL(index int, info URLInfo, driver *agouti.WebDriver, done *sync.WaitGroup) error {
	defer done.Done()

	page, err := driver.NewPage(agouti.Browser("phantomjs"))
	if err != nil {
		log.WithFields(log.Fields{
			"index": index,
			"err":   err,
		}).Warn("Failed to create session")
		return err
	}
	defer page.Destroy()

	page.Session().SetPageLoad(300000)
	page.Session().SetScriptTimeout(30000)
	page.Session().SetImplicitWait(0)

	url := info.URL
	log.WithFields(log.Fields{
		"index": index,
		"url":   url,
	}).Debug("Get target url")

	// this step may be blocked until 'page load' timeout
	if err := page.Navigate(url); err != nil {
		log.WithFields(log.Fields{
			"index": index,
			"url":   url,
			"err":   err,
		}).Warn("Failed to navigate to target url")
		return err
	}
	log.WithFields(log.Fields{
		"index": index,
		"url":   url,
	}).Debug("Success to open url")

	var res map[string]interface{}
	for i, jsFunc := range info.JsFuncs {
		err := page.RunScript(jsFunc, res, &res)
		if err != nil {
			log.WithFields(log.Fields{
				"index":       index,
				"url":         url,
				"scriptIndex": i,
				"err":         err,
			}).Warn("Failed to run script")
			return err
		}
		log.WithFields(log.Fields{
			"index":       index,
			"url":         url,
			"scriptIndex": i,
			"res":         res,
		}).Debug("Get parse result")

		if val, ok := res["stop"]; ok && val.(bool) {
			return nil
		}

		if val, ok := res["waitTime"]; ok {
			delete(res, "stop")
			delete(res, "waitTime")
			d := time.Duration(val.(float64)) * time.Millisecond
			time.Sleep(d)
		}
	}
	log.WithFields(log.Fields{
		"index": index,
		"url":   url,
	}).Debug("Parse finished")

	if len(res) == 0 {
		log.WithFields(log.Fields{
			"index": index,
			"url":   url,
		}).Debug("Get empty response")
		return nil
	}

	data, _ := json.Marshal(res)
	info.Ofile.WriteString(fmt.Sprintf("%s\t%s\n", url, string(data)))

	return nil
}

func walkFile(infoChan chan<- URLInfo, done *sync.WaitGroup) func(path string, f os.FileInfo, err error) error {
	info := new(URLInfo)
	return func(path string, f os.FileInfo, err error) error {
		if f.IsDir() {
			return nil
		}
		log.WithFields(log.Fields{
			"path":     path,
			"fileName": f.Name(),
		}).Debug("Get crawler data file")

		// find script
		filename := f.Name()
		if !viper.IsSet(filename) {
			log.WithFields(log.Fields{
				"filename": filename,
			}).Warn("No conf item for data file")
			return nil
		}
		conf := viper.GetStringMap(filename)
		log.WithFields(log.Fields{
			"filename": filename,
			"conf":     conf,
		}).Debug("Read conf for data file")
		if val, ok := conf["ignore"]; ok && val.(bool) {
			log.WithFields(log.Fields{
				"conf": conf,
			}).Warn("Data file is ignored by conf")
			return nil
		}

		res_path := fOutputDir + "/" + conf["output_file"].(string)
		file, err := os.Create(res_path)
		if err != nil {
			log.WithFields(log.Fields{
				"res_path": res_path,
				"err":      err,
			}).Warn("Failed to create output file")
			return nil
		}
		info.Ofile = file

		// we can have multi scripts for one page
		var script_path []string
		switch scripts := conf["script_name"].(type) {
		case string:
			script_path = append(script_path, fScriptDir+"/"+scripts)
		case []interface{}:
			for _, v := range scripts {
				script_path = append(script_path, fScriptDir+"/"+v.(string))
			}
		default:
			log.WithFields(log.Fields{
				"script_path": scripts,
			}).Warn("Conf[script_name] is not string or array of string")
			return nil
		}

		// load scripts
		for _, v := range script_path {
			data, err := ioutil.ReadFile(v)
			if err != nil {
				log.WithFields(log.Fields{
					"script_path": v,
					"err":         err,
				}).Warn("Failed to read script")
				return nil
			}
			info.JsFuncs = append(info.JsFuncs, string(data))
		}

		// read url from data file
		file, err = os.Open(path)
		if err != nil {
			log.WithFields(log.Fields{
				"path": path,
				"err":  err,
			}).Fatal("Failed to open data file")

			return nil
		}
		defer file.Close()

		sc := bufio.NewScanner(file)
		for sc.Scan() {
			info.URL = sc.Text()
			infoChan <- *info
			done.Add(1)
		}

		return nil
	}
}

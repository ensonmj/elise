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
	"golang.org/x/sync/errgroup"
)

var (
	fParallel  int
	fVerbose   int
	fDataDir   string
	fScriptDir string
	fOutputDir string
	fLogDir    string
)

func init() {
	HamalCmd.AddCommand(versionCmd)
	HamalCmd.AddCommand(picCmd)

	pflags := HamalCmd.PersistentFlags()
	pflags.IntVarP(&fParallel, "parallel", "p", 10, "max number of parallel exector")
	pflags.IntVarP(&fVerbose, "verbose", "v", 4, "log level: 0~5, 5 for debug detail")
	pflags.StringVar(&fLogDir, "logDir", "./log", "dir for storage log")
	viper.BindPFlag("parallel", pflags.Lookup("parallel"))
	viper.BindPFlag("verbose", pflags.Lookup("verbose"))
	viper.BindPFlag("logDir", pflags.Lookup("logDir"))

	flags := HamalCmd.Flags()
	flags.StringVar(&fDataDir, "dataDir", "./data", "dir for storage url files")
	flags.StringVar(&fScriptDir, "scriptDir", "./script", "dir for storage scripts")
	flags.StringVar(&fOutputDir, "outputDir", "./output", "dir for storage parse result files")
	viper.BindPFlag("dataDir", flags.Lookup("dataDir"))
	viper.BindPFlag("scriptDir", flags.Lookup("scriptDir"))
	viper.BindPFlag("outputDir", flags.Lookup("outputDir"))
}

type URLInfo struct {
	URL     string
	Ofile   *os.File
	JsFuncs []string
}

// HamalCmd is the top entrance of hamal
var HamalCmd = &cobra.Command{
	Use:   "hamal",
	Short: "Hamal parse webpage based on scripts.",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if _, err := os.Stat(fLogDir); os.IsNotExist(err) {
			os.Mkdir(fLogDir, os.ModePerm)
		}
		log.AddHook(lfshook.NewHook(lfshook.PathMap{
			log.DebugLevel: fLogDir + "/debug.log",
			log.InfoLevel:  fLogDir + "/info.log",
			log.WarnLevel:  fLogDir + "/warn.log",
			log.FatalLevel: fLogDir + "/fatal.log",
		}))
		log.SetLevel(log.Level(fVerbose))
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		if _, err := os.Stat(fDataDir); os.IsNotExist(err) {
			log.WithFields(log.Fields{
				"dataDir": fDataDir,
			}).Fatal("No script dir")
			return err
		}
		if _, err := os.Stat(fScriptDir); os.IsNotExist(err) {
			log.WithFields(log.Fields{
				"scriptDir": fScriptDir,
			}).Fatal("No script dir")
			return err
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
	var eg errgroup.Group

	done := new(sync.WaitGroup)
	infoChan := make(chan URLInfo, fParallel)
	retryNum := fParallel/2 + 1
	retryChan := make(chan URLInfo, retryNum)
	for i := 0; i < fParallel; i++ {
		index := i
		eg.Go(func() error {
		RESTART:
			driver := agouti.PhantomJS()
			if err := driver.Start(); err != nil {
				log.WithFields(log.Fields{
					"index": index,
					"err":   err,
				}).Fatalf("Failed to start driver:%v", err)
				return err
			}
			log.WithField("index", index).Debug("Success to start worker")

			for {
				select {
				case info, ok := <-infoChan:
					if !ok {
						log.WithField("index", index).Debug("Worker exit")
						driver.Stop()
						return nil
					}

					err := parseURL(index, info, driver)
					if err != nil {
						// sometimes phantomjs crashed or just navigate timeout
						// we can't differentiate cause of errors
						// so we just restart the worker and push the *info* to retry queue
						log.WithFields(log.Fields{
							"index": index,
							"info":  info,
						}).Warn("Failed to parse, will retry later")
						go func(info URLInfo) {
							time.Sleep(10 * time.Second)
							retryChan <- info
						}(info)

						goto RESTART
					}

					log.WithField("url", info.URL).Info("Success to parse")
					done.Done()
				}
			}
		})
	}
	for i := 0; i < retryNum; i++ {
		index := i
		eg.Go(func() error {
		RESTART:
			driver := agouti.PhantomJS()
			if err := driver.Start(); err != nil {
				log.WithFields(log.Fields{
					"index": index,
					"err":   err,
				}).Fatalf("Failed to start driver:%v", err)
				return err
			}
			log.WithField("index", index).Debug("Success to start retry worker")

			for {
				select {
				case info, ok := <-retryChan:
					if !ok {
						log.WithField("index", index).Debug("Retry worker exit")
						driver.Stop()
						return nil
					}
					err := parseURL(index, info, driver)
					if err != nil {
						// failed to retry, no more retry for this url, just mark completed
						log.WithField("url", info.URL).Info("Failed to retry")
						done.Done()

						// we need restart retry worker
						goto RESTART
					}
					log.WithField("url", info.URL).Info("Success to parse")
					done.Done()
				}
			}
		})
	}

	log.WithField("dataDir", fDataDir).Debug("Start to traversal data files")
	filepath.Walk(fDataDir, walkFile(infoChan, done))

	done.Wait()
	close(infoChan)
	close(retryChan)
	eg.Wait()
	log.Debug("Finish all tasks")

	return nil
}

func parseURL(index int, info URLInfo, driver *agouti.WebDriver) error {
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

		resPath := fOutputDir + "/" + conf["output_file"].(string)
		file, err := os.Create(resPath)
		if err != nil {
			log.WithFields(log.Fields{
				"res_path": resPath,
				"err":      err,
			}).Warn("Failed to create output file")
			return nil
		}
		info.Ofile = file

		// we can have multi scripts for one page
		var scriptPath []string
		switch scripts := conf["script_name"].(type) {
		case string:
			scriptPath = append(scriptPath, fScriptDir+"/"+scripts)
		case []interface{}:
			for _, v := range scripts {
				scriptPath = append(scriptPath, fScriptDir+"/"+v.(string))
			}
		default:
			log.WithFields(log.Fields{
				"script_name": scripts,
			}).Warn("Conf[script_name] is not string or array of string")
			return nil
		}

		// load scripts
		for _, v := range scriptPath {
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

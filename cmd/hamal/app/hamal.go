package app

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/rifflock/lfshook"
	"github.com/sclevine/agouti"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

var (
	fSplitCount int
	fParallel   int
	fVerbose    int
	fDataDir    string
	fScriptDir  string
	fOutputDir  string
	fLogDir     string
	fFlushLog   bool
)

func init() {
	HamalCmd.AddCommand(versionCmd)
	HamalCmd.AddCommand(picCmd)

	pflags := HamalCmd.PersistentFlags()
	pflags.IntVarP(&fParallel, "parallel", "p", 10, "max number of parallel exector")
	pflags.IntVarP(&fVerbose, "verbose", "v", 4, "log level: 0~5, 5 for debug detail")
	pflags.StringVar(&fLogDir, "logDir", "./log", "dir for storage log")
	pflags.BoolVar(&fFlushLog, "flushLog", false, "flush log dir for debug")
	viper.BindPFlag("parallel", pflags.Lookup("parallel"))
	viper.BindPFlag("verbose", pflags.Lookup("verbose"))
	viper.BindPFlag("logDir", pflags.Lookup("logDir"))
	viper.BindPFlag("flushLog", pflags.Lookup("flushLog"))

	flags := HamalCmd.Flags()
	flags.IntVarP(&fSplitCount, "splitCount", "c", 10000, "max line count for one output file")
	flags.StringVar(&fDataDir, "dataDir", "./data", "dir for storage url files")
	flags.StringVar(&fScriptDir, "scriptDir", "./script", "dir for storage scripts")
	flags.StringVar(&fOutputDir, "outputDir", "./output", "dir for storage parse result files")
	viper.BindPFlag("splitCount", flags.Lookup("splitCount"))
	viper.BindPFlag("dataDir", flags.Lookup("dataDir"))
	viper.BindPFlag("scriptDir", flags.Lookup("scriptDir"))
	viper.BindPFlag("outputDir", flags.Lookup("outputDir"))
}

type FileInfo struct {
	Filename string
	Line     uint64
	Start    time.Time
	Done     *sync.WaitGroup // just include parse, exclude write file
}

type URLRes struct {
	URL string
	Res map[string]interface{}
}

type URLInfo struct {
	URL      string
	JsFuncs  []string
	DumpHTML bool
	ResChan  chan URLRes
	FInfo    *FileInfo
}

// HamalCmd is the top entrance of hamal
var HamalCmd = &cobra.Command{
	Use:   "hamal",
	Short: "Hamal parse webpage based on scripts.",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if _, err := os.Stat(fLogDir); os.IsNotExist(err) {
			os.Mkdir(fLogDir, os.ModePerm)
		} else if fFlushLog {
			dir, err := os.Open(fLogDir)
			if err != nil {
				return err
			}
			defer dir.Close()
			names, err := dir.Readdirnames(-1)
			if err != nil {
				return err
			}
			for _, name := range names {
				err = os.RemoveAll(filepath.Join(fLogDir, name))
				if err != nil {
					return err
				}
			}
		}
		log.AddHook(lfshook.NewHook(lfshook.PathMap{
			log.DebugLevel: filepath.Join(fLogDir, "debug.log"),
			log.InfoLevel:  filepath.Join(fLogDir, "info.log"),
			log.WarnLevel:  filepath.Join(fLogDir, "warn.log"),
			log.FatalLevel: filepath.Join(fLogDir, "fatal.log"),
		}))
		log.SetLevel(log.Level(fVerbose))
		return nil
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
	var infoeg errgroup.Group
	var retryeg errgroup.Group

	infoChan := make(chan URLInfo, fParallel)
	retryNum := fParallel/2 + 1
	retryChan := make(chan URLInfo, retryNum)
	for i := 0; i < fParallel; i++ {
		index := i
		infoeg.Go(func() error {
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

						driver.Stop()
						goto RESTART
					}

					log.WithField("url", info.URL).Info("Success to parse")
					info.FInfo.Done.Done()
				}
			}
		})
	}
	for i := 0; i < retryNum; i++ {
		index := i
		retryeg.Go(func() error {
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
						info.FInfo.Done.Done()

						// we need restart retry worker
						driver.Stop()
						goto RESTART
					}
					log.WithField("url", info.URL).Info("Success to parse")
					info.FInfo.Done.Done()
				}
			}
		})
	}

	log.WithField("dataDir", fDataDir).Debug("Start to traversal data files")
	err := filepath.Walk(fDataDir, walkFile(infoChan))
	close(infoChan)
	infoeg.Wait()
	close(retryChan)
	retryeg.Wait()
	log.Debug("Finish all tasks")

	if err != nil {
		return err
	}
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

	res := make(map[string]interface{})
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

	if info.DumpHTML {
		res["html"], err = page.HTML()
		if err != nil {
			log.WithFields(log.Fields{
				"index": index,
				"url":   url,
			}).Warn("Failed to get html")
			return err
		}
	}

	if len(res) == 0 {
		log.WithFields(log.Fields{
			"index": index,
			"url":   url,
		}).Debug("Get empty response")
		return nil
	}

	info.ResChan <- URLRes{URL: url, Res: res}

	return nil
}

func walkFile(infoChan chan<- URLInfo) func(path string, f os.FileInfo, err error) error {
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

		// get 'dump' setting
		if val, ok := conf["dump_html"]; ok && val.(bool) {
			info.DumpHTML = val.(bool)
			log.WithField("dumpHTML", info.DumpHTML).Debug("Read dump_html conf")
		}

		// we can have multi scripts for one page
		if _, ok := conf["script_name"]; !ok {
			log.WithFields(log.Fields{
				"conf": conf,
			}).Warn("Data file's conf has no 'script_name' item")
			return nil
		}
		var scriptPath []string
		switch scripts := conf["script_name"].(type) {
		case string:
			scriptPath = append(scriptPath, filepath.Join(fScriptDir, scripts))
		case []interface{}:
			for _, v := range scripts {
				scriptPath = append(scriptPath, filepath.Join(fScriptDir, v.(string)))
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

		ctx, cancel := context.WithCancel(context.Background())
		// write output file routine
		resChan := make(chan URLRes, fParallel+fParallel/2+1)
		// TODO: make sure finish to write output file before exit
		go func() {
			// create output file
			var resFilename string
			if val, ok := conf["output_file"]; ok {
				resFilename = val.(string)
			} else {
				resFilename = filename
			}
			noSuffix := strings.TrimSuffix(resFilename, filepath.Ext(resFilename))
			resPath := filepath.Join(fOutputDir, noSuffix+".txt")
			resFile, err := os.Create(resPath)
			if err != nil {
				log.WithFields(log.Fields{
					"res_path": resPath,
					"err":      err,
				}).Warn("Failed to create output file")
				cancel()
				return
			}

			line := 0
			index := 0
			for res := range resChan {
				data, _ := json.Marshal(res.Res)
				resFile.WriteString(fmt.Sprintf("%s\t%s\n", res.URL, string(data)))
				line++
				if line >= fSplitCount {
					resFile.Close()
					line = 0
					index++
					resPath = filepath.Join(fOutputDir, noSuffix+"_"+strconv.Itoa(index)+".txt")
					resFile, err = os.Create(resPath)
					if err != nil {
						log.WithFields(log.Fields{
							"res_path": resPath,
							"err":      err,
						}).Warn("Failed to create output file")
						cancel()
						break
					}
				}
			}
			resFile.Close()
		}()

		// read url from data file
		inFile, err := os.Open(path)
		if err != nil {
			log.WithFields(log.Fields{
				"path": path,
				"err":  err,
			}).Fatal("Failed to open data file")
			return nil
		}
		defer inFile.Close()

		fi := FileInfo{
			Filename: filename,
			Start:    time.Now(),
			Done:     new(sync.WaitGroup),
		}
		info.FInfo = &fi
		log.WithFields(log.Fields{
			"filename": filename,
			"start":    info.FInfo.Start,
		}).Info("Start to crawler urls in one file")

		sc := bufio.NewScanner(inFile)
		for sc.Scan() {
			select {
			case <-ctx.Done():
				log.WithFields(log.Fields{
					"filename": filename,
					"line":     atomic.LoadUint64(&info.FInfo.Line),
					"elapsed":  time.Since(info.FInfo.Start),
				}).Info("Partial finished to crawler urls in one file")

				return ctx.Err()
			default:
				info.FInfo.Done.Add(1)
				info.URL = sc.Text()
				info.ResChan = resChan
				atomic.AddUint64(&info.FInfo.Line, 1)
				// log.WithField("info", info).Debug("Create one info")
				infoChan <- *info
			}
		}

		go func() {
			info.FInfo.Done.Wait()
			close(resChan)
			log.WithFields(log.Fields{
				"filename": filename,
				"line":     atomic.LoadUint64(&info.FInfo.Line),
				"elapsed":  time.Since(info.FInfo.Start),
			}).Info("Finished to crawler urls in one file")
		}()

		return nil
	}
}

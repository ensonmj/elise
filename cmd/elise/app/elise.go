package app

import (
	"os"
	"path/filepath"

	log "github.com/Sirupsen/logrus"
	"github.com/ensonmj/elise/cmd/elise/util"
	"github.com/rifflock/lfshook"
	"github.com/spf13/cobra"
)

var (
	fEliseParallel  int
	fEliseVerbose   int
	fEliseLogDir    string
	fEliseFlushLog  bool
	fEliseInPath    string
	fEliseOutputDir string
	fEliseSplitCnt  int
)

func init() {
	EliseCmd.AddCommand(VersionCmd)
	EliseCmd.AddCommand(CrawlCmd)
	EliseCmd.AddCommand(PicCmd)
	EliseCmd.AddCommand(WebCmd)
	EliseCmd.AddCommand(ConvCmd)

	pflags := EliseCmd.PersistentFlags()
	pflags.IntVarP(&fEliseParallel, "parallel", "P", 10, "max number of parallel exector")
	pflags.IntVarP(&fEliseVerbose, "verbose", "v", 0, "log level: 0~5, 5 for debug detail")
	pflags.StringVarP(&fEliseLogDir, "logDir", "L", "./log", "dir for storage log")
	pflags.BoolVar(&fEliseFlushLog, "flushLog", false, "flush log dir for debug")
	pflags.StringVarP(&fEliseInPath, "inPath", "p", "-", "file or dir for input, '-' stands for term")
	pflags.StringVarP(&fEliseOutputDir, "outputDir", "O", "./output", "dir for storage result files")
	pflags.IntVarP(&fEliseSplitCnt, "splitCount", "c", 1000, "max line count for one output file")
}

var EliseCmd = &cobra.Command{
	Use:   "elise",
	Short: "Elise crawl webpage based on javascript, then parse and demonstrate",
	Long:  "Elise, the queue of spiders, one of the heroes of game League of Legends(LOL).",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if _, err := os.Stat(fEliseLogDir); os.IsNotExist(err) {
			if err = os.Mkdir(fEliseLogDir, os.ModePerm); err != nil {
				return err
			}
		} else if fEliseFlushLog {
			if err := util.FlushDir(fEliseLogDir); err != nil {
				return err
			}
		}
		log.AddHook(lfshook.NewHook(lfshook.PathMap{
			log.DebugLevel: filepath.Join(fEliseLogDir, "debug.log"),
			log.InfoLevel:  filepath.Join(fEliseLogDir, "info.log"),
			log.WarnLevel:  filepath.Join(fEliseLogDir, "warn.log"),
			log.FatalLevel: filepath.Join(fEliseLogDir, "fatal.log"),
		}))
		log.SetLevel(log.Level(fEliseVerbose))

		if fEliseInPath != "-" {
			if _, err := os.Stat(fEliseInPath); os.IsNotExist(err) {
				return err
			}
		}
		if _, err := os.Stat(fEliseOutputDir); os.IsNotExist(err) {
			if err = os.Mkdir(fEliseOutputDir, os.ModePerm); err != nil {
				return err
			}
		}
		return nil
	},
}

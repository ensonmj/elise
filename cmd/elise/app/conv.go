package app

import (
	"bytes"
	"encoding/json"
	"errors"
	html "html/template"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	text "text/template"

	"github.com/ensonmj/elise/cmd/elise/assets"
	"github.com/ensonmj/elise/textline"
	"github.com/spf13/cobra"
)

var (
	fConvDevMode  bool
	fConvTmplSafe bool
	fConvTmplFile string
	fConvFileExt  string
	fConvDelim    string
	fConvField    int
)

func init() {
	flags := ConvCmd.Flags()
	flags.BoolVarP(&fConvDevMode, "devMode", "D", false, "develop mode, using local assets")
	flags.BoolVarP(&fConvTmplSafe, "tmplSafe", "s", false, "safe mode, using html/template or text/template")
	flags.StringVarP(&fConvTmplFile, "tmplFile", "t", "/assets/templates/conv.tmpl", "template file")
	flags.StringVarP(&fConvFileExt, "fileExt", "e", ".xml", "output file extension")
	flags.StringVarP(&fConvDelim, "delimiter", "d", "\t", "field delimiter")
	flags.IntVarP(&fConvField, "field", "f", 2, "nth field for conversion, index start from 1")
}

type LineWorker struct{}

func (w *LineWorker) Process(line []byte) (interface{}, error) {
	fields := bytes.Split(line, []byte(fConvDelim))
	if len(fields) < fConvField {
		return nil, errors.New("line format is wrong")
	}

	var data interface{}
	if err := json.Unmarshal(fields[fConvField-1], &data); err != nil {
		return nil, err
	}

	return data, nil
}

type FileWorker struct {
	outputDir     string
	splitCnt      int
	tmplSafe      bool
	textTmpl      *text.Template
	htmlTmpl      *html.Template
	isTerm        bool
	termPreWrite  bool
	termPostWrite bool
	noSuffix      string
	ext           string
	index         int
	file          *os.File
}

func (w *FileWorker) PrepareOnce() error {
	if w.tmplSafe {
		tmpl, err := initHtmlTmpl(fConvDevMode, fConvTmplFile)
		if err != nil {
			return err
		}
		w.htmlTmpl = tmpl
		return nil
	}

	tmpl, err := initTextTmpl(fConvDevMode, fConvTmplFile)
	if err != nil {
		return err
	}
	w.textTmpl = tmpl
	return nil
}

func (w *FileWorker) BeforeWrite(fn string) error {
	if fn == "-" {
		w.isTerm = true
		return nil
	}

	base := filepath.Base(fn)
	w.noSuffix = strings.TrimSuffix(base, filepath.Ext(base))
	return nil
}

func (w *FileWorker) PreWrite(row int) error {
	if w.isTerm {
		if !w.termPreWrite {
			w.termPreWrite = true
			w.file = os.Stdout
			if w.tmplSafe {
				return w.htmlTmpl.ExecuteTemplate(w.file, "header", nil)
			}
			return w.textTmpl.ExecuteTemplate(w.file, "header", nil)
		}
		return nil
	}

	if row%w.splitCnt == 0 {
		index := row / w.splitCnt
		var path string
		if index > 0 {
			path = filepath.Join(w.outputDir, w.noSuffix+"_"+strconv.Itoa(index)+w.ext)
		} else {
			path = filepath.Join(w.outputDir, w.noSuffix+w.ext)
		}
		f, err := os.Create(path)
		if err != nil {
			return err
		}

		w.file = f
		w.index = index
		if w.tmplSafe {
			return w.htmlTmpl.ExecuteTemplate(w.file, "header", nil)
		}
		return w.textTmpl.ExecuteTemplate(w.file, "header", nil)
	}
	return nil
}

func (w *FileWorker) Write(data interface{}) error {
	if w.tmplSafe {
		return w.htmlTmpl.ExecuteTemplate(w.file, "item", nil)
	}

	return w.textTmpl.ExecuteTemplate(w.file, "item", data)
}

func (w *FileWorker) PostWrite(row int) error {
	if w.isTerm {
		if !w.termPostWrite {
			w.termPostWrite = true
			if w.tmplSafe {
				return w.htmlTmpl.ExecuteTemplate(w.file, "footer", nil)
			}
			return w.textTmpl.ExecuteTemplate(w.file, "footer", nil)
		}
		return nil
	}

	if row%w.splitCnt == w.splitCnt-1 && w.file != nil {
		var err error
		if w.tmplSafe {
			err = w.htmlTmpl.ExecuteTemplate(w.file, "footer", nil)
		} else {
			err = w.textTmpl.ExecuteTemplate(w.file, "footer", nil)
		}
		w.file.Close()
		w.file = nil
		return err
	}
	return nil
}

func (w *FileWorker) AfterWrite() error {
	if w.isTerm {
		return nil
	}

	w.file.Close()
	w.file = nil
	// remove extra file if exists
	index := w.index
	for {
		index++
		path := filepath.Join(w.outputDir, w.noSuffix+"_"+strconv.Itoa(index)+w.ext)
		if err := os.Remove(path); os.IsNotExist(err) {
			break
		}
	}

	return nil
}

var ConvCmd = &cobra.Command{
	Use:   "conv",
	Short: "Conv data from json to other format based on template.",
	RunE: func(cmd *cobra.Command, args []string) error {
		return conv()
	},
}

func conv() error {
	tlm := textline.New(fEliseInPath, fEliseParallel, &LineWorker{},
		&FileWorker{
			outputDir: fEliseOutputDir,
			splitCnt:  fEliseSplitCnt,
			tmplSafe:  fConvTmplSafe,
			ext:       fConvFileExt,
		})
	tlm.FeedLine()
	tlm.Wait()

	return nil
}

func initTextTmpl(devMode bool, filePath string) (*text.Template, error) {
	tmplStr, err := assets.FSString(devMode, filePath)
	if err != nil {
		return nil, err
	}
	return text.New("tmpl").Parse(tmplStr)
}

func initHtmlTmpl(devMode bool, filePath string) (*html.Template, error) {
	tmplStr, err := assets.FSString(devMode, filePath)
	if err != nil {
		return nil, err
	}
	return html.New("tmpl").Parse(tmplStr)
}

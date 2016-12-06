package app

import (
	"bytes"
	"encoding/json"
	html "html/template"
	"os"
	text "text/template"

	"github.com/Sirupsen/logrus"
	"github.com/ensonmj/elise/cmd/elise/assets"
	"github.com/ensonmj/fileproc"
	"github.com/spf13/cobra"
)

var (
	fConvTmplSafe bool
	fConvTmplFile string
	fConvFileExt  string
	fConvDelim    string
	fConvField    int
)

func init() {
	flags := ConvCmd.Flags()
	flags.BoolVarP(&fConvTmplSafe, "tmplSafe", "s", false, "safe mode, using html/template or text/template")
	flags.StringVarP(&fConvTmplFile, "tmplFile", "t", "/assets/templates/conv.tmpl", "template file")
	flags.StringVarP(&fConvFileExt, "fileExt", "e", ".xml", "output file extension")
	flags.StringVarP(&fConvDelim, "delimiter", "d", "\t", "field delimiter")
	flags.IntVarP(&fConvField, "field", "f", 2, "nth field for conversion, index start from 1")
}

type convProcessor struct {
	tmplSafe bool
	textTmpl *text.Template
	htmlTmpl *html.Template
}

func (w *convProcessor) Map(line []byte) []byte {
	fields := bytes.Split(line, []byte(fConvDelim))
	if len(fields) < fConvField {
		return nil
	}

	var data interface{}
	d := json.NewDecoder(bytes.NewReader(fields[fConvField-1]))
	d.UseNumber()
	if err := d.Decode(&data); err != nil {
		return nil
	}

	var buf bytes.Buffer
	if w.tmplSafe {
		w.htmlTmpl.ExecuteTemplate(&buf, "item", data)
	} else {
		w.textTmpl.ExecuteTemplate(&buf, "item", data)
	}

	return buf.Bytes()
}

func newConvProcessor(tmplSafe bool) *convProcessor {
	w := &convProcessor{tmplSafe: tmplSafe}

	if w.tmplSafe {
		tmpl, err := initHtmlTmpl(fEliseDevMode, fConvTmplFile)
		if err != nil {
			return nil
		}
		w.htmlTmpl = tmpl
		return w
	}

	tmpl, err := initTextTmpl(fEliseDevMode, fConvTmplFile)
	if err != nil {
		return nil
	}
	w.textTmpl = tmpl
	return w
}

type tmplWrapper struct {
	tmplSafe bool
	textTmpl *text.Template
	htmlTmpl *html.Template
}

func (w *tmplWrapper) BeforeWrite(f *os.File) error {
	if w.tmplSafe {
		return w.htmlTmpl.ExecuteTemplate(f, "header", nil)
	}
	return w.textTmpl.ExecuteTemplate(f, "header", nil)
}

func (w *tmplWrapper) AfterWrite(f *os.File) error {
	if w.tmplSafe {
		return w.htmlTmpl.ExecuteTemplate(f, "footer", nil)
	}
	return w.textTmpl.ExecuteTemplate(f, "footer", nil)
}

func newTmplWrapper(tmplSafe bool) *tmplWrapper {
	w := &tmplWrapper{tmplSafe: tmplSafe}

	if w.tmplSafe {
		tmpl, err := initHtmlTmpl(fEliseDevMode, fConvTmplFile)
		if err != nil {
			return nil
		}
		w.htmlTmpl = tmpl
		return w
	}

	tmpl, err := initTextTmpl(fEliseDevMode, fConvTmplFile)
	if err != nil {
		return nil
	}
	w.textTmpl = tmpl

	return w
}

var ConvCmd = &cobra.Command{
	Use:   "conv",
	Short: "Conv data from json to other format based on template.",
	RunE: func(cmd *cobra.Command, args []string) error {
		return conv()
	},
}

func conv() error {
	m := newConvProcessor(fConvTmplSafe)
	fw := newTmplWrapper(fConvTmplSafe)
	if fEliseInPath == "-" {
		return fileproc.ProcTerm(fEliseParallel, fEliseBufMaxSize, m, nil, fw)
	}
	fp := fileproc.NewFileProcessor(fEliseParallel, fEliseBufMaxSize, fEliseSplitCnt, true, false, m, nil, fw)
	err := fp.ProcPath(fEliseInPath, fEliseOutputDir, fConvFileExt)
	i, mc, r := fp.Stat()
	logrus.WithFields(logrus.Fields{
		"inputLineCnt": i,
		"mapOutCnt":    mc,
		"redOutCnt":    r,
	}).Debug("Finished all work")
	return err
}

func initTextTmpl(devMode bool, filePath string) (*text.Template, error) {
	tmplStr, err := assets.FSString(devMode, filePath)
	if err != nil {
		return nil, err
	}
	return text.New("tmpl").Funcs(text.FuncMap{
		"add":     add,
		"marshal": marshal,
	}).Parse(tmplStr)
}

func add(a, b int) int {
	return a + b
}

func marshal(v interface{}) html.JS {
	data, _ := json.Marshal(v)
	return html.JS(data)
}

func initHtmlTmpl(devMode bool, filePath string) (*html.Template, error) {
	tmplStr, err := assets.FSString(devMode, filePath)
	if err != nil {
		return nil, err
	}
	return html.New("tmpl").Parse(tmplStr)
}

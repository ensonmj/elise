package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/PuerkitoBio/goquery"
	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/yosssi/gohtml"
	"golang.org/x/net/html"
	"golang.org/x/sync/errgroup"
)

type TextInfo struct {
	LineCnt *uint64
	Text    string // format: "'url'\t'HTML'"
}

type CrawlerResp struct {
	LandingPage string `json:"final_url"`
	// Title       string `json:"title"`
	HTML string `json:"html"`
}

type ImgItem struct {
	Src                  string
	Width, Height, Ratio float64
}

type ScoredGrp struct {
	Score    int
	ImgItems []ImgItem
	grpNode  *html.Node
}

type ScoredGrpSlice struct {
	LP     string
	Title  string
	ImgSGs []*ScoredGrp
}

func (sgs ScoredGrpSlice) Len() int {
	return len(sgs.ImgSGs)
}

func (sgs ScoredGrpSlice) Swap(i, j int) {
	sgs.ImgSGs[i], sgs.ImgSGs[j] = sgs.ImgSGs[j], sgs.ImgSGs[i]
}

// descending order
func (sgs ScoredGrpSlice) Less(i, j int) bool {
	return sgs.ImgSGs[j].Score < sgs.ImgSGs[i].Score
}

func (sgs ScoredGrpSlice) Length() int {
	return len(sgs.ImgSGs)
}

type PicDesc struct {
	Title        string   `json:"title"`
	Images       []string `json:"moreImages"`
	allScoredGrp ScoredGrpSlice
}

var headerTmpl string = `
{{- define "header" }}
<!DOCTYPE html>
<html>
	<head>
		<style type="text/css">
		img {width: 100px; height: 100px; margin-right: 10px}
		</style>
		<link rel="stylesheet" href="http://cdn.bootcss.com/bootstrap/3.3.0/css/bootstrap.min.css">
		<link rel="stylesheet" href="http://cdn.bootcss.com/bootstrap/3.3.0/css/bootstrap-theme.min.css">
		<script src="http://cdn.bootcss.com/jquery/1.11.1/jquery.min.js"></script>
		<script src="http://cdn.bootcss.com/bootstrap/3.3.0/js/bootstrap.min.js"></script>
	</head>
	<body>
		<div class="list-group container">
{{end -}}
`
var itemTmpl string = `
{{define "item"}}
			<div class="row list-group list-group-item panel panel-primary">
				<div class="list-group-item panel-heading">
					<a class="panel-title" target="_blank" href="{{.LP}}">{{.Title}}</a>
				</div>
				{{- range .ImgSGs}}
				<div class="list-group-item">
					<span class="badge">{{.Score}}</span>
					{{- range .ImgItems}}
					<img src="{{.Src}}" prim-width="{{.Width}}" prim-height="{{.Height}}" width-height-ratio="{{.Ratio}}">
					{{- end}}
				</div>
				{{- end}}
			</div>
{{ end -}}
`
var footerTmpl string = `
{{define "footer"}}
		</div>
	</body>
</html>
{{ end -}}
`

var PicCmd = &cobra.Command{
	Use:   "pic",
	Short: "Use pictures to describe the webpage.",
	Long: `Check all pictures in the webpage, find the pictures which can best
represent the webpage according to web structure and something else.`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if _, err := os.Stat(fPubDir); os.IsNotExist(err) {
			if err = os.Mkdir(fPubDir, os.ModePerm); err != nil {
				return err
			}
		}
		if fCrawlerFile == "" && fURL == "" {
			return errors.New("Must specify 'crawlerFile' or 'url'")
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return parsePage()
	},
}

var (
	fURL         string
	fHTMLDoc     string
	fHTMLFile    string
	fCrawlerFile string
	fPubDir      string
	fOTrim       bool
	fWidthMin    float64
	fHeightMin   float64
	fRatioMin    float64 // width / height
	fRatioMax    float64
	fPicSplitCnt int
	fPicParallel int
)

func init() {
	flags := PicCmd.Flags()
	flags.StringVarP(&fURL, "url", "u", "", "webpage url for parse")
	flags.StringVarP(&fHTMLDoc, "htmlDoc", "d", "", "HTML content, must be utf-8 encoding")
	flags.StringVarP(&fHTMLFile, "htmlFile", "F", "", "HTML file, must be utf-8 encoding")
	flags.StringVarP(&fCrawlerFile, "crawlerFile", "f", "", "crawler result file")
	flags.StringVarP(&fPubDir, "pubDir", "P", "./pub", "public dir for store demonstration HTML file")
	flags.BoolVarP(&fOTrim, "outputTrim", "o", false, "print HTML after trimming")
	flags.Float64VarP(&fWidthMin, "widthMin", "W", 64.0, "image min width")
	flags.Float64VarP(&fHeightMin, "heightMin", "H", 64.0, "image min height")
	flags.Float64VarP(&fRatioMin, "ratioMin", "r", 0.35, "image width/height min value")
	flags.Float64VarP(&fRatioMax, "ratioMax", "R", 2.85, "image width/height max value")
	flags.IntVarP(&fPicSplitCnt, "splitCount", "c", 100, "max line count for one output file")
	flags.IntVarP(&fPicParallel, "parallel", "p", 10, "max number of parallel exector")
	viper.BindPFlag("url", flags.Lookup("url"))
	viper.BindPFlag("htmlDoc", flags.Lookup("htmlDoc"))
	viper.BindPFlag("htmlFile", flags.Lookup("htmlFile"))
	viper.BindPFlag("crawlerFile", flags.Lookup("crawlerFile"))
	viper.BindPFlag("pubDir", flags.Lookup("pubDir"))
	viper.BindPFlag("outputTrim", flags.Lookup("outputTrim"))
	viper.BindPFlag("widthMin", flags.Lookup("widthMin"))
	viper.BindPFlag("heightMin", flags.Lookup("heightMin"))
	viper.BindPFlag("ratioMin", flags.Lookup("ratioMin"))
	viper.BindPFlag("ratioMax", flags.Lookup("ratioMax"))
	viper.BindPFlag("splitCount", flags.Lookup("splitCount"))
	viper.BindPFlag("parallel", flags.Lookup("parallel"))
}

func parsePage() error {
	if fCrawlerFile != "" {
		var eg, writeEG errgroup.Group
		textInfo := TextInfo{LineCnt: new(uint64)}
		textInfoChan := make(chan TextInfo, fPicParallel)
		picDescChan := make(chan *PicDesc, fPicParallel)
		jobStarted := time.Now()
		for i := 0; i < fPicParallel; i++ {
			index := i
			eg.Go(func() error {
				for {
					select {
					case textInfo, ok := <-textInfoChan:
						log.WithFields(log.Fields{
							"index": index,
							"text":  textInfo.Text,
						}).Debug("Received text")
						if !ok {
							log.WithField("index", index).Debug("Worker exit")
							return nil
						}
						fields := strings.Split(textInfo.Text, "\t")
						if len(fields) != 2 {
							log.WithFields(log.Fields{
								"index": index,
								"text":  textInfo.Text,
							}).Warn("Text format is wrong")
							continue
						}
						var resp CrawlerResp
						if err := json.Unmarshal([]byte(fields[1]), &resp); err != nil {
							log.WithFields(log.Fields{
								"index": index,
								"text":  textInfo.Text,
								"err":   err,
							}).Warn("Failed to unmarshal")
							continue
						}
						doc, err := goquery.NewDocumentFromReader(strings.NewReader(resp.HTML))
						if err != nil {
							log.WithFields(log.Fields{
								"index": index,
								"err":   err,
							}).Warn("Failed to create document")
							continue
						}

						title := doc.Find("title").Text()
						lp := resp.LandingPage

						trimHTML(doc)
						if err = normalizeHTML(doc, lp); err != nil {
							log.WithFields(log.Fields{
								"index": index,
								"err":   err,
							}).Warn("Failed to normalize HTML")
							continue
						}
						trimBranch(doc)
						if fOTrim {
							str, _ := doc.Html()
							fmt.Printf("%s\037%s\036\n", lp, gohtml.Format(str))
						}

						picDesc, err := groupImg(doc)
						if err != nil {
							log.WithFields(log.Fields{
								"index": index,
								"err":   err,
							}).Warn("Failed to group image")
							continue
						}
						picDesc.Title = title
						picDesc.allScoredGrp.LP = lp
						picDesc.allScoredGrp.Title = title
						log.WithFields(log.Fields{
							"index":   index,
							"picDesc": picDesc,
						}).Debug("Finished to parse one HTML string")

						atomic.AddUint64(textInfo.LineCnt, 1)
						picDescChan <- picDesc
					}
				}
			})
		}

		f, err := os.Open(fCrawlerFile)
		if err != nil {
			log.WithFields(log.Fields{
				"crawlerFile": fCrawlerFile,
				"err":         err,
			}).Fatal("Failed to open crawler result file")
			return err
		}

		// create output HTML file
		ctx, cancel := context.WithCancel(context.Background())
		writeEG.Go(func() error {
			base := filepath.Base(fCrawlerFile)
			noSuffix := strings.TrimSuffix(base, filepath.Ext(base))
			resPath := filepath.Join(fPubDir, noSuffix+".html")
			f, tmpl, err := openHTML(resPath)
			if err != nil {
				log.WithFields(log.Fields{
					"resPath": resPath,
					"err":     err,
				}).Warn("Failed to create output HTML file")
				cancel()
			}

			line := 0
			index := 0
			for picDesc := range picDescChan {
				err = produceHTML(f, tmpl, picDesc.allScoredGrp)
				if err != nil {
					log.WithFields(log.Fields{
						"resPath": resPath,
						"err":     err,
					}).Warn("Failed to produce HTML node")
					cancel()
				}

				line++
				if line >= fPicSplitCnt {
					closeHTML(f, tmpl)

					line = 0
					index++
					resPath = filepath.Join(fPubDir, noSuffix+"_"+strconv.Itoa(index)+".html")
					f, tmpl, err = openHTML(resPath)
					if err != nil {
						log.WithFields(log.Fields{
							"resPath": resPath,
							"err":     err,
						}).Warn("Failed to create output HTML file")
						cancel()
					}
				}
			}
			closeHTML(f, tmpl)
			return nil
		})

		sc := bufio.NewScanner(f)
		sc.Buffer([]byte{}, 2*1024*1024) // default 64k, change to 2M
		lineCount := 0
	SCANLOOP:
		for sc.Scan() {
			select {
			case <-ctx.Done():
				log.WithFields(log.Fields{
					"filename":     fCrawlerFile,
					"writeLineCnt": atomic.LoadUint64(textInfo.LineCnt),
					"elapsed":      time.Since(jobStarted),
					"err":          ctx.Err(),
				}).Warn("Partial finished to extract img from one file")

				break SCANLOOP
			default:
				textInfo.Text = sc.Text()
				textInfoChan <- textInfo
				lineCount++
			}
		}
		f.Close()
		close(textInfoChan)
		eg.Wait()

		close(picDescChan)
		writeEG.Wait()

		if err = sc.Err(); err != nil {
			log.WithFields(log.Fields{
				"file":         fCrawlerFile,
				"readLineCnt":  lineCount,
				"writeLineCnt": atomic.LoadUint64(textInfo.LineCnt),
				"elapsed":      time.Since(jobStarted),
				"err":          err,
			}).Warn("Failed to read line from file")
			return err
		}
		log.WithFields(log.Fields{
			"file":         fCrawlerFile,
			"readLineCnt":  lineCount,
			"writeLineCnt": atomic.LoadUint64(textInfo.LineCnt),
			"elapsed":      time.Since(jobStarted),
		}).Debug("Finished all the job")
	} else if fURL != "" {
		var err error
		var doc *goquery.Document
		if fHTMLDoc != "" {
			doc, err = goquery.NewDocumentFromReader(strings.NewReader(fHTMLDoc))
			if err != nil {
				log.WithError(err).Fatal("Failed to create document")
				return err
			}
		} else if fHTMLFile != "" {
			f, err := os.Open(fHTMLFile)
			if err != nil {
				log.WithFields(log.Fields{
					"HTMLFile": fHTMLFile,
					"err":      err,
				}).Fatal("Failed to open HTML file")
				return err
			}
			defer f.Close()

			doc, err = goquery.NewDocumentFromReader(f)
			if err != nil {
				log.WithError(err).Fatal("Failed to create document")
				return err
			}
		} else {
			doc, err = goquery.NewDocument(fURL)
			if err != nil {
				log.WithError(err).Fatal("Failed to create document")
				return err
			}
		}

		title := doc.Find("title").Text()
		trimHTML(doc)
		if err = normalizeHTML(doc, fURL); err != nil {
			log.WithError(err).Warn("Failed to normalize HTML")
			return err
		}
		trimBranch(doc)
		if fOTrim {
			str, _ := doc.Html()
			fmt.Printf("%s\037%s\036\n", fURL, gohtml.Format(str))
		}

		picDesc, err := groupImg(doc)
		if err != nil {
			log.WithError(err).Warn("Failed to group image")
			return err
		}
		picDesc.Title = title
		picDesc.allScoredGrp.LP = fURL
		picDesc.allScoredGrp.Title = title
		log.WithField("picDesc", picDesc).Debug("Finished to parse one HTML")

		noSuffix := strings.TrimSuffix(fCrawlerFile, filepath.Ext(fCrawlerFile))
		resPath := filepath.Join(fPubDir, noSuffix+".html")
		f, tmpl, err := openHTML(resPath)
		if err != nil {
			log.WithFields(log.Fields{
				"resPath": resPath,
				"err":     err,
			}).Fatal("Failed to create output HTML file")
			return err
		}
		defer closeHTML(f, tmpl)
		err = produceHTML(f, tmpl, picDesc.allScoredGrp)
		if err != nil {
			log.WithError(err).Warn("Failed to produce HTML node")
			return err
		}
		log.Debug("Finished to write output HTML file")
	}

	return nil
}

// trim some node according selector
func trimHTML(doc *goquery.Document) {
	for _, selector := range []string{"head", "header", "footer", "aside",
		"a", "script", "object", "nav", "form", "input", "style", "iframe",
		"h1", "h2", "h3", "h4", "h5", "h6"} {
		doc.Find(selector).Remove()
	}
}

func normalizeHTML(doc *goquery.Document, lpSrc string) error {
	num := 0
	doc.Find("img").Each(func(i int, sel *goquery.Selection) {
		for _, n := range sel.Nodes {
			var imgSrc string
			for _, attr := range n.Attr {
				if attr.Key == "data-src" || attr.Key == "data-original" {
					imgSrc = attr.Val
					break
				} else if attr.Key == "src" {
					imgSrc = attr.Val
				}
			}
			if imgSrc == "" {
				var buf bytes.Buffer
				html.Render(&buf, n)
				log.WithFields(log.Fields{
					"lpSrc": lpSrc,
					"node":  n,
					"HTML":  buf.String(),
				}).Warn("Can't find img src while normalizing")
				continue
			}

			lpURL, err := url.Parse(lpSrc)
			if err != nil {
				log.WithFields(log.Fields{
					"lpSrc": lpSrc,
					"err":   err,
				}).Warn("Failed to parse landing page url")
				continue
			}
			imgURL, err := url.Parse(imgSrc)
			if err != nil {
				log.WithFields(log.Fields{
					"imgSrc": imgSrc,
					"err":    err,
				}).Warn("Failed to parse img url")
				continue
			}

			absoluteImgSrc := lpURL.ResolveReference(imgURL).String()
			n.Attr = append(n.Attr, html.Attribute{Key: "prim-img", Val: absoluteImgSrc})
			log.WithFields(log.Fields{
				"absoluteImgSrc": absoluteImgSrc,
			}).Debug("Got img src")

			num++
		}
	})

	if num <= 0 {
		return errors.New("can't find any img node")
	}
	return nil
}

// trim branch which not include img node or unqualified img
func trimBranch(doc *goquery.Document) {
	// only one body node
	sel := doc.Find("body")
	if len(sel.Nodes) == 0 {
		return
	}
	trimNode(sel.Nodes[0], func(n *html.Node) bool {
		// trim TextNode, CommentNode etc, which is not ElementNode
		if n.Type != html.ElementNode {
			return true
		}
		if n.FirstChild != nil {
			return false
		}
		// trim leaf node which is not img node
		if n.Data != "img" {
			return true
		}
		// trim img node which is not so good
		return filterImg(n)
	})
}

// trim node which not include img node
func trimNode(n *html.Node, rmCheck func(n *html.Node) bool) {
	var next *html.Node
	for c := n.FirstChild; c != nil; c = next {
		next = c.NextSibling
		trimNode(c, rmCheck)
	}
	if rmCheck(n) {
		n.Parent.RemoveChild(n)
	}
}

func filterImg(n *html.Node) bool {
	if filterImgbyRect(n) {
		return true
	}

	return filterImgbyExt(n)
}

func filterImgbyRect(n *html.Node) bool {
	_, _, width, height := getImgRect(n)
	if width < fWidthMin || height < fHeightMin {
		log.WithFields(log.Fields{
			"width":  width,
			"height": height,
		}).Debug("Filtered by width or height")
		return true
	}
	ratio := width / height
	if ratio < fRatioMin || ratio > fRatioMax {
		log.WithFields(log.Fields{
			"width":  width,
			"height": height,
		}).Debug("Filtered by width/height ratio")
		return true
	}

	return false
}

func getImgRect(n *html.Node) (top, left, width, height float64) {
	for _, attr := range n.Attr {
		switch attr.Key {
		case "prim-top":
			top, _ = strconv.ParseFloat(attr.Val, 64)
		case "prim-left":
			left, _ = strconv.ParseFloat(attr.Val, 64)
		case "prim-width":
			width, _ = strconv.ParseFloat(attr.Val, 64)
		case "prim-height":
			height, _ = strconv.ParseFloat(attr.Val, 64)
		}
	}
	return
}

func filterImgbyExt(n *html.Node) bool {
	var imgSrc string
	for _, attr := range n.Attr {
		if attr.Key == "prim-img" {
			imgSrc = attr.Val
			break
		}
	}
	if imgSrc == "" {
		var buf bytes.Buffer
		html.Render(&buf, n)
		log.WithFields(log.Fields{
			"node": n,
			"HTML": buf.String(),
		}).Warn("Can't find img src while filtering")
		return true
	}
	// some img has no extention
	ext := filepath.Ext(imgSrc)
	log.WithFields(log.Fields{
		"imgSrc": imgSrc,
		"ext":    ext,
	}).Debug("Get img extention")
	if ext == ".gif" {
		return true
	}

	return false
}

func groupImg(doc *goquery.Document) (*PicDesc, error) {
	sel := doc.Find("body")
	if len(sel.Nodes) == 0 {
		return nil, errors.New("empty HTML body")
	}
	// only one body node
	grpImgs := splitTree(sel.Nodes[0])
	allScoredGrp := calcScore(grpImgs)
	if allScoredGrp.Length() <= 0 {
		return nil, errors.New("can't find any img node")
	}
	sort.Sort(allScoredGrp)

	picDesc := &PicDesc{allScoredGrp: allScoredGrp}
	return picDesc, nil
}

func splitTree(root *html.Node) []*html.Node {
	if !needSplit(root) {
		return []*html.Node{root}
	}
	for root.FirstChild.NextSibling == nil {
		root = root.FirstChild
	}
	var grpImgs []*html.Node

	newRoot := &html.Node{Type: html.ElementNode, Data: "div"}
	begin := root.FirstChild
	var end, next *html.Node
	for curr := root.FirstChild; curr != nil; curr = next {
		curr.Parent = newRoot
		next = curr.NextSibling

		if next != nil {
			if nodeEqual(curr, next) {
				continue
			}
			curr.NextSibling = nil
			next.PrevSibling = nil
		}

		end = curr
		newRoot.FirstChild = begin
		if begin != end {
			newRoot.LastChild = end
		}
		grpImgs = append(grpImgs, newRoot)

		begin = next
		newRoot = &html.Node{Type: html.ElementNode, Data: "div"}
	}

	var allGrpImgs []*html.Node
	for _, n := range grpImgs {
		if !needSplit(n) {
			allGrpImgs = append(allGrpImgs, n)
			continue
		}
		allGrpImgs = append(allGrpImgs, splitTree(n)...)
	}

	return allGrpImgs
}

// please make sure c,n not nil
func nodeEqual(c, n *html.Node) bool {
	if c.Data != n.Data {
		return false
	}
	if c.Data == "img" {
		return nodeSizeEqual(c, n)
	}
	if countSubNode(c) != countSubNode(n) {
		return false
	}

	ccurr := c.FirstChild
	ncurr := n.FirstChild
	for ccurr != nil && ncurr != nil {
		if !nodeEqual(ccurr, ncurr) {
			return false
		}
		ccurr = ccurr.NextSibling
		ncurr = ncurr.NextSibling
	}

	return true
}

func nodeSizeEqual(c, n *html.Node) bool {
	_, _, cw, ch := getImgRect(c)
	_, _, nw, nh := getImgRect(n)

	if cw != nw || ch != nh {
		return false
	}
	return true
}

func countSubNode(n *html.Node) int {
	num := 0
	for curr := n.FirstChild; curr != nil; curr = curr.NextSibling {
		num++
	}

	return num
}

func needSplit(n *html.Node) bool {
	if n == nil || n.FirstChild == nil {
		return false
	} else if n.FirstChild.NextSibling == nil {
		return needSplit(n.FirstChild)
	}

	var next *html.Node
	for curr := n.FirstChild; curr != n.LastChild; curr = next {
		next = curr.NextSibling
		if !nodeEqual(curr, next) {
			return true
		}
	}

	return false
}

func calcScore(grpImgs []*html.Node) ScoredGrpSlice {
	var allScoredGrp ScoredGrpSlice
	for _, n := range grpImgs {
		var imgItems []ImgItem
		var num int
		num, imgItems = extractImg(n, imgItems)
		if num == 0 {
			continue
		}

		grp := &ScoredGrp{Score: num, ImgItems: imgItems, grpNode: n}
		allScoredGrp.ImgSGs = append(allScoredGrp.ImgSGs, grp)
	}
	return allScoredGrp
}

func extractImg(n *html.Node, imgItems []ImgItem) (int, []ImgItem) {
	if n.Data == "img" {
		var imgItem ImgItem
		for _, attr := range n.Attr {
			switch attr.Key {
			case "prim-width":
				imgItem.Width, _ = strconv.ParseFloat(attr.Val, 64)
			case "prim-height":
				imgItem.Height, _ = strconv.ParseFloat(attr.Val, 64)
			case "prim-img":
				imgItem.Src = attr.Val
			}
		}
		imgItem.Ratio = imgItem.Width / imgItem.Height
		imgItems = append(imgItems, imgItem)
		return 1, imgItems
	}

	totalNum := 0
	for curr := n.FirstChild; curr != nil; curr = curr.NextSibling {
		var items []ImgItem
		num := 0
		num, items = extractImg(curr, items)
		totalNum += num
		imgItems = append(imgItems, items...)
	}
	return totalNum, imgItems
}

func openHTML(filename string) (f *os.File, tmpl *template.Template, err error) {
	f, err = os.Create(filename)
	if err != nil {
		return
	}

	tmpl, err = template.New("tmpl").Parse(headerTmpl)
	if err != nil {
		return
	}
	tmpl, err = tmpl.Parse(itemTmpl)
	if err != nil {
		return
	}
	tmpl, err = tmpl.Parse(footerTmpl)
	if err != nil {
		return
	}

	err = tmpl.ExecuteTemplate(f, "header", nil)
	if err != nil {
		return
	}

	return
}

func produceHTML(f *os.File, tmpl *template.Template, sgs ScoredGrpSlice) error {
	err := tmpl.ExecuteTemplate(f, "item", sgs)
	if err != nil {
		return err
	}
	return nil
}

func closeHTML(f *os.File, tmpl *template.Template) error {
	defer f.Close()

	err := tmpl.ExecuteTemplate(f, "footer", nil)
	if err != nil {
		return err
	}
	return nil
}

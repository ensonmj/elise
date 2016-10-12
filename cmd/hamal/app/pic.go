package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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

var (
	fURL         string
	fHTMLDoc     string
	fHTMLFile    string
	fCrawlerFile string
	fCompact     int
	fCheckImg    bool
	fOTrim       bool
)

func init() {
	flags := picCmd.Flags()
	flags.StringVarP(&fURL, "url", "u", "", "webpage url for parse")
	flags.StringVarP(&fHTMLDoc, "htmlDoc", "d", "", "html content, must be utf-8 encoding")
	flags.StringVarP(&fHTMLFile, "htmlFile", "f", "", "html file, must be utf-8 encoding")
	flags.StringVarP(&fCrawlerFile, "crawlerFile", "c", "", "crawler result file")
	flags.IntVarP(&fCompact, "compact", "n", 5, "compactness of html node")
	flags.BoolVar(&fCheckImg, "checkImg", true, "filter img by size, extention etc.")
	flags.BoolVar(&fOTrim, "oTrim", false, "print html after trimming")
	viper.BindPFlag("url", flags.Lookup("url"))
	viper.BindPFlag("htmlDoc", flags.Lookup("htmlDoc"))
	viper.BindPFlag("htmlFile", flags.Lookup("htmlFile"))
	viper.BindPFlag("crawlerFile", flags.Lookup("crawlerFile"))
	viper.BindPFlag("compact", flags.Lookup("compact"))
	viper.BindPFlag("checkImg", flags.Lookup("checkImg"))
	viper.BindPFlag("oTrim", flags.Lookup("oTrim"))
}

type TextInfo struct {
	LineCnt *uint64
	Text    string // format: "url\thtml"
}

type CrawlerResp struct {
	LandingPage string `json:"final_url"`
	// Title       string `json:"title"`
	HTML string `json:"html"`
}

type ScoredGrp struct {
	Score   int
	ImgURLs []string
	grpNode *html.Node
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

type PicDesc struct {
	Title        string   `json:"title"`
	Images       []string `json:"moreImages"`
	allScoredGrp ScoredGrpSlice
}

var picCmd = &cobra.Command{
	Use:   "pic",
	Short: "Use pictures to describe the webpage.",
	Long: `Check all pictures in the webpage, find the pictures which can best
represent the webpage according to web structure and something else.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return parsePage()
	},
}

var headerTmpl string = `{{define "header"}}<!DOCTYPE html>
	<html>
		<head>
			<style type="text/css">
			* {margin:0; padding:0; list-style:none;}
			.row ul:after {content:" "; display:block; clear:both; height:0; width:0;}
			.row ul li {width:1000px; float:left; margin-bottom:10px; margin-right:10px;}
			.row ul img {width: 100px;}
			.row ul li p {line-height: 22px;}
			</style>
		</head>
		<body>
	{{- end -}}
	`
var itemTmpl string = `{{define "item"}}
			<div class="row">
				<a href="{{.LP}}">
					<p>{{.Title}}</p>
				</a>
				<ul>
				{{- range .ImgSGs}}
					<li>
						<p>Score: {{.Score}}</p>
						{{- range .ImgURLs}}
						<img src="{{.}}">
						{{- end}}
					</li>
				{{- end}}
				</ul>
			</div>
	{{- end -}}
	`

var footerTmpl string = `{{define "footer"}}
		</body>
	</html>
	{{- end -}}
	`

func parsePage() error {
	if fCrawlerFile != "" {
		var eg, writeEG errgroup.Group
		textInfo := TextInfo{LineCnt: new(uint64)}
		textInfoChan := make(chan TextInfo, fParallel)
		picDescChan := make(chan *PicDesc, fParallel)
		jobStarted := time.Now()
		for i := 0; i < fParallel; i++ {
			index := i
			eg.Go(func() error {
				for {
					select {
					case textInfo, ok := <-textInfoChan:
						if !ok {
							log.WithField("index", index).Debug("worker exit")
							return nil
						}
						fields := strings.Split(textInfo.Text, "\t")
						var resp CrawlerResp
						if err := json.Unmarshal([]byte(fields[1]), &resp); err != nil {
							continue
						}
						doc, err := goquery.NewDocumentFromReader(strings.NewReader(resp.HTML))
						if err != nil {
							log.WithField("err", err).Warn("Failed to create document")
							continue
						}

						title := doc.Find("title").Text()

						trimHTML(resp.LandingPage, doc)

						picDesc, err := groupImg(doc)
						picDesc.Title = title
						picDesc.allScoredGrp.LP = resp.LandingPage
						picDesc.allScoredGrp.Title = title
						log.WithFields(log.Fields{
							"picDesc": picDesc,
							"err":     err,
						}).Debug("Finished to parse body")
						if err != nil {
							continue
						}

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

		// create output html file
		ctx, cancel := context.WithCancel(context.Background())
		writeEG.Go(func() error {
			base := filepath.Base(fCrawlerFile)
			noSuffix := strings.TrimSuffix(base, filepath.Ext(base))
			resPath := filepath.Join(fOutputDir, noSuffix+".html")
			f, tmpl, err := openHTML(resPath)
			if err != nil {
				log.WithFields(log.Fields{
					"resPath": resPath,
					"err":     err,
				}).Warn("Failed to create output html file")
				cancel()
				return err
			}

			line := 0
			index := 0
			for picDesc := range picDescChan {
				err = produceHTML(f, tmpl, picDesc.allScoredGrp)
				if err != nil {
					log.WithFields(log.Fields{
						"resPath": resPath,
						"err":     err,
					}).Warn("Failed to produce html node")
					cancel()
					break
				}

				line++
				if line >= 100 {
					closeHTML(f, tmpl)

					line = 0
					index++
					resPath = filepath.Join(fOutputDir, noSuffix+"_"+strconv.Itoa(index)+".html")
					f, tmpl, err = openHTML(resPath)
					if err != nil {
						log.WithFields(log.Fields{
							"resPath": resPath,
							"err":     err,
						}).Warn("Failed to create output html file")
						cancel()
						break
					}
				}
			}
			closeHTML(f, tmpl)
			return nil
		})

		sc := bufio.NewScanner(f)
		sc.Buffer([]byte{}, 2*1024*1024)
		lineCount := 0
		for sc.Scan() {
			select {
			case <-ctx.Done():
				log.WithFields(log.Fields{
					"filename":     fCrawlerFile,
					"writeLineCnt": atomic.LoadUint64(textInfo.LineCnt),
					"elapsed":      time.Since(jobStarted),
					"err":          ctx.Err(),
				}).Info("Partial finished to extract img from one file")

				break
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
				"file":        fCrawlerFile,
				"readLineCnt": lineCount,
				"err":         err,
			}).Warn("Read line from file")
			return err
		}
		log.WithFields(log.Fields{
			"file":        fCrawlerFile,
			"readLineCnt": lineCount,
		}).Debug("Read line from file")
	} else if fURL != "" {
		var err error
		var doc *goquery.Document
		if fHTMLDoc != "" {
			doc, err = goquery.NewDocumentFromReader(strings.NewReader(fHTMLDoc))
			if err != nil {
				log.WithField("err", err).Fatal("Failed to create document")
				return err
			}
		} else if fHTMLFile != "" {
			f, err := os.Open(fHTMLFile)
			if err != nil {
				log.WithFields(log.Fields{
					"htmlFile": fHTMLFile,
					"err":      err,
				}).Fatal("Failed to open html file")
				return err
			}
			defer f.Close()

			doc, err = goquery.NewDocumentFromReader(f)
			if err != nil {
				log.WithField("err", err).Fatal("Failed to create document")
				return err
			}
		} else {
			doc, err = goquery.NewDocument(fURL)
			if err != nil {
				log.WithField("err", err).Fatal("Failed to create document")
				return err
			}
		}

		trimHTML(fURL, doc)

		picDesc, err := groupImg(doc)
		picDesc.allScoredGrp.LP = fURL
		log.WithFields(log.Fields{
			"picDesc": picDesc,
			"err":     err,
		}).Debug("Finished to parse body")
		if err != nil {
			return err
		}

		noSuffix := strings.TrimSuffix(fCrawlerFile, filepath.Ext(fCrawlerFile))
		resPath := filepath.Join(fOutputDir, noSuffix+".html")
		f, tmpl, err := openHTML(resPath)
		if err != nil {
			log.WithFields(log.Fields{
				"resPath": resPath,
				"err":     err,
			}).Fatal("Failed to create output html file")
			return err
		}
		defer closeHTML(f, tmpl)
		err = produceHTML(f, tmpl, picDesc.allScoredGrp)
		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Warn("Failed to produce html node")
			return err
		}
	}

	return nil
}

func trimHTML(lpSrc string, doc *goquery.Document) {
	// trim some node according selector
	for _, selector := range []string{"head", "header", "footer", "aside",
		"a", "script", "object", "nav", "form", "input", "style", "iframe",
		"h1", "h2", "h3", "h4", "h5", "h6"} {
		doc.Find(selector).Remove()
	}

	doc.Find("body").Each(func(i int, sel *goquery.Selection) {
		// only one body node
		body := sel.Nodes[0]
		// trim nodes which not include img node
		trimNode(body, func(n *html.Node) bool {
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
			// img node has no children
			if !normalizeImg(n, lpSrc) {
				return true
			}
			// trim img node which is not so good
			if fCheckImg {
				return filterImg(n, lpSrc)
			}

			return false
		})
	})

	if fOTrim {
		str, _ := doc.Html()
		fmt.Printf("%s\037%s\036\n", lpSrc, gohtml.Format(str))
	}
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

func normalizeImg(n *html.Node, lpSrc string) bool {
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
			"html":  buf.String(),
		}).Warn("Can't find img src while normalizing")
		return false
	}

	lpURL, err := url.Parse(lpSrc)
	if err != nil {
		log.WithFields(log.Fields{
			"lpSrc": lpSrc,
			"err":   err,
		}).Warn("Failed to parse landing page url")
		return false
	}
	imgURL, err := url.Parse(imgSrc)
	if err != nil {
		log.WithFields(log.Fields{
			"imgSrc": imgSrc,
			"err":    err,
		}).Warn("Failed to parse img url")
		return false
	}

	absoluteImgSrc := lpURL.ResolveReference(imgURL).String()
	n.Attr = append(n.Attr, html.Attribute{Key: "prim-img", Val: absoluteImgSrc})
	log.WithFields(log.Fields{
		"absoluteImgSrc": absoluteImgSrc,
	}).Debug("Got img src")

	return true
}

func filterImg(n *html.Node, lpSrc string) bool {
	if filterImgbyRect(n) {
		return true
	}

	return filterImgbyExt(n, lpSrc)
}

func filterImgbyRect(n *html.Node) bool {
	_, _, width, height := getImgRect(n)
	if width < 100 || height < 100 {
		log.WithFields(log.Fields{
			"width":  width,
			"height": height,
		}).Debug("Filtered by width or height")
		return true
	}
	ratio := width / height
	if ratio < 0.35 || ratio > 2.85 {
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

func filterImgbyExt(n *html.Node, lpSrc string) bool {
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
			"lpSrc": lpSrc,
			"node":  n,
			"html":  buf.String(),
		}).Warn("Can't find img src while filtering")
		return true
	}
	// some img has no extention
	ext := filepath.Ext(imgSrc)
	if ext == "gif" {
		log.WithField("imgSrc", imgSrc).Debug("Img filtered by ext")
		return true
	}

	return false
}

func groupImg(doc *goquery.Document) (*PicDesc, error) {
	picDesc := &PicDesc{}

	doc.Find("body").Each(func(i int, sel *goquery.Selection) {
		// only one body node
		body := sel.Nodes[0]
		grpImgs := splitTree(body)
		allScoredGrp := calcGrpScore(grpImgs)
		sort.Sort(allScoredGrp)
		picDesc.allScoredGrp = allScoredGrp
	})

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

func calcGrpScore(grpImgs []*html.Node) ScoredGrpSlice {
	var allScoredGrp ScoredGrpSlice
	for _, n := range grpImgs {
		var imgURLs []string
		var num int
		num, imgURLs = countImgNode(n, imgURLs)

		grp := &ScoredGrp{Score: num, ImgURLs: imgURLs, grpNode: n}
		allScoredGrp.ImgSGs = append(allScoredGrp.ImgSGs, grp)
	}
	return allScoredGrp
}

func countImgNode(n *html.Node, imgURLs []string) (int, []string) {
	if n.Data == "img" {
		for _, attr := range n.Attr {
			if attr.Key == "prim-img" {
				imgURLs = append(imgURLs, attr.Val)
				break
			}
		}
		return 1, imgURLs
	}

	totalNum := 0
	for curr := n.FirstChild; curr != nil; curr = curr.NextSibling {
		var imgs []string
		num := 0
		num, imgs = countImgNode(curr, imgs)
		totalNum += num
		imgURLs = append(imgURLs, imgs...)
	}
	return totalNum, imgURLs
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

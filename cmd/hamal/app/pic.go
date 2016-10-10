package app

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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
	fPrintHTML   bool
)

func init() {
	flags := picCmd.Flags()
	flags.StringVarP(&fURL, "url", "u", "", "webpage url for parse")
	flags.StringVarP(&fHTMLDoc, "htmlDoc", "d", "", "html content, must be utf-8 encoding")
	flags.StringVarP(&fHTMLFile, "htmlFile", "f", "", "html file, must be utf-8 encoding")
	flags.StringVarP(&fCrawlerFile, "crawlerFile", "c", "", "crawler result file")
	flags.IntVarP(&fCompact, "compact", "n", 5, "compactness of html node")
	flags.BoolVar(&fCheckImg, "checkImg", true, "filter img by size, extention etc.")
	flags.BoolVar(&fPrintHTML, "printHTML", false, "print html after trimming")
	viper.BindPFlag("url", flags.Lookup("url"))
	viper.BindPFlag("htmlDoc", flags.Lookup("htmlDoc"))
	viper.BindPFlag("htmlFile", flags.Lookup("htmlFile"))
	viper.BindPFlag("crawlerFile", flags.Lookup("crawlerFile"))
	viper.BindPFlag("compact", flags.Lookup("compact"))
	viper.BindPFlag("checkImg", flags.Lookup("checkImg"))
	viper.BindPFlag("printHTML", flags.Lookup("printHTML"))
}

type CrawlerResp struct {
	LandingPage string `json:"final_url"`
	Title       string `json:"title"`
	HTML        string `json:"html"`
}

type ImgGroup struct {
	score float32
	imgs  []string
}

type PicDesc struct {
	Title   string   `json:"title"`
	Images  []string `json:"moreImages"`
	grpImgs []ImgGroup
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

func parsePage() error {
	if fCrawlerFile != "" {
		var eg errgroup.Group

		textChan := make(chan string, fParallel)
		for i := 0; i < fParallel; i++ {
			index := i
			eg.Go(func() error {
				for {
					select {
					case text, ok := <-textChan:
						if !ok {
							log.WithField("index", index).Debug("worker exit")
							return nil
						}
						fields := strings.Split(text, "\t")
						var resp CrawlerResp
						if err := json.Unmarshal([]byte(fields[1]), &resp); err != nil {
							continue
						}
						doc, err := goquery.NewDocumentFromReader(strings.NewReader(resp.HTML))
						if err != nil {
							log.WithField("err", err).Warn("Failed to create document")
							continue
						}

						trimHTML(resp.LandingPage, doc)

						picDesc, err := groupImg(doc)
						log.WithFields(log.Fields{
							"picDesc": picDesc,
							"err":     err,
						}).Debug("Finished to parse body")
						if err != nil {
							continue
						}
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
		sc := bufio.NewScanner(f)
		sc.Buffer([]byte{}, 2*1024*1024)
		lineCount := 0
		for sc.Scan() {
			line := sc.Text()
			textChan <- line
			lineCount++
		}
		f.Close()
		close(textChan)

		eg.Wait()

		if err = sc.Err(); err != nil {
			log.WithFields(log.Fields{
				"file": fCrawlerFile,
				"line": lineCount,
				"err":  err,
			}).Warn("Read line from file")
		} else {
			log.WithFields(log.Fields{
				"file": fCrawlerFile,
				"line": lineCount,
				"err":  err,
			}).Debug("Read line from file")
		}
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
		log.WithFields(log.Fields{
			"picDesc": picDesc,
			"err":     err,
		}).Debug("Finished to parse body")
		if err != nil {
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

	if fPrintHTML {
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
	ratio := float64(width) / float64(height)
	if ratio < 0.35 || ratio > 2.85 {
		log.WithFields(log.Fields{
			"width":  width,
			"height": height,
		}).Debug("Filtered by width/height ratio")
		return true
	}

	return false
}

func getImgRect(n *html.Node) (top, left, width, height int) {
	for _, attr := range n.Attr {
		switch attr.Key {
		case "prim-top":
			top, _ = strconv.Atoi(attr.Val)
		case "prim-left":
			left, _ = strconv.Atoi(attr.Val)
		case "prim-width":
			width, _ = strconv.Atoi(attr.Val)
		case "prim-height":
			height, _ = strconv.Atoi(attr.Val)
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
	picDesc := &PicDesc{Title: doc.Find("title").Text()}

	return picDesc, nil
}

package app

import (
	"bufio"
	"encoding/json"
	"net/url"
	"os"
	"strings"

	"github.com/PuerkitoBio/goquery"
	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/net/html"
	"golang.org/x/sync/errgroup"
)

var (
	fURL         string
	fHTMLDoc     string
	fHTMLFile    string
	fCrawlerFile string
)

func init() {
	flags := picCmd.Flags()
	flags.StringVarP(&fURL, "url", "u", "", "webpage url for parse")
	flags.StringVarP(&fHTMLDoc, "htmlDoc", "d", "", "html content, must be utf-8 encoding")
	flags.StringVarP(&fHTMLFile, "htmlFile", "f", "", "html file, must be utf-8 encoding")
	flags.StringVarP(&fCrawlerFile, "crawlerFile", "c", "", "crawler result file")
	viper.BindPFlag("url", flags.Lookup("url"))
	viper.BindPFlag("htmlDoc", flags.Lookup("htmlDoc"))
	viper.BindPFlag("htmlFile", flags.Lookup("htmlFile"))
	viper.BindPFlag("crawlerFile", flags.Lookup("crawlerFile"))
}

type CrawlerResp struct {
	LandingPage string `json:"final_url"`
	Title       string `json:"title"`
	HTML        string `json:"html"`
}

type PicDesc struct {
	Title  string   `json:"title"`
	Images []string `json:"moreImages"`
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

		respChan := make(chan CrawlerResp, fParallel)
		for i := 0; i < fParallel; i++ {
			index := i
			eg.Go(func() error {
				for {
					select {
					case resp, ok := <-respChan:
						if !ok {
							log.WithField("index", index).Debug("worker exit")
							return nil
						}
						doc, err := goquery.NewDocumentFromReader(strings.NewReader(resp.HTML))
						if err != nil {
							log.WithField("err", err).Warn("Failed to create document")
							continue
						}
						pd, err := parseBody(resp.LandingPage, resp.Title, doc)
						log.WithFields(log.Fields{
							"pd":  pd,
							"err": err,
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
		for sc.Scan() {
			line := sc.Text()
			fields := strings.Split(line, "\t")
			// origURL := fields[0]
			var resp CrawlerResp
			if err := json.Unmarshal([]byte(fields[1]), &resp); err != nil {
				continue
			}
			respChan <- resp
		}
		f.Close()
		close(respChan)

		eg.Wait()
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

		pd, err := parseBody(fURL, "", doc)
		log.WithFields(log.Fields{
			"pd":  pd,
			"err": err,
		}).Debug("Finished to parse body")
		if err != nil {
			return err
		}
	}

	return nil
}

func parseBody(lpSrc, lpTitle string, doc *goquery.Document) (PicDesc, error) {
	var pd PicDesc

	lpURL, err := url.Parse(lpSrc)
	if err != nil {
		log.WithFields(log.Fields{
			"lpSrc": lpSrc,
			"err":   err,
		}).Warn("Failed to parse url")
		return pd, err
	}

	if lpTitle != "" {
		pd.Title = lpTitle
	} else {
		pd.Title = doc.Find("title").Text()
	}
	doc.Find("body").Each(func(i int, sel *goquery.Selection) {
		for _, n := range sel.Nodes {
			traverseNode(n, func(n *html.Node) {
				if n.Type == html.ElementNode && n.Data == "img" {
					var imgSrc string
					for _, attr := range n.Attr {
						if attr.Key == "data-src" || attr.Key == "data-original" {
							imgSrc = attr.Val
							break
						} else if attr.Key == "src" {
							imgSrc = attr.Val
						}
					}
					if imgSrc != "" {
						imgURL, err := url.Parse(imgSrc)
						if err != nil {
							log.WithFields(log.Fields{
								"imgSrc": imgSrc,
								"err":    err,
							}).Warn("Failed to parse url")
							return
						}
						absoluteImgSrc := lpURL.ResolveReference(imgURL).String()
						log.WithFields(log.Fields{
							"lpSrc":          lpSrc,
							"imgSrc":         imgSrc,
							"absoluteImgSrc": absoluteImgSrc,
						}).Debug("Got img src")
						pd.Images = append(pd.Images, absoluteImgSrc)
					}
				}
			})
		}
	})

	return pd, nil
}

func traverseNode(n *html.Node, f func(n *html.Node)) {
	f(n)

	for c := n.FirstChild; c != nil; c = c.NextSibling {
		traverseNode(c, f)
	}
}

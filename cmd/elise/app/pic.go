package app

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/PuerkitoBio/goquery"
	log "github.com/Sirupsen/logrus"
	"github.com/ensonmj/elise/cmd/elise/conf"
	"github.com/ensonmj/elise/htmlutil"
	"github.com/ensonmj/fileproc"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/yosssi/gohtml"
	"golang.org/x/net/html"
)

var (
	fWidthMin  float64
	fHeightMin float64
	fRatioMin  float64 // width / height
	fRatioMax  float64
	fImgNumMin int
	fOTrim     bool
	fPicDelim  string
	fPicField  int
)

func init() {
	flags := PicCmd.Flags()
	flags.Float64VarP(&fWidthMin, "widthMin", "W", 64.0, "image min width")
	flags.Float64VarP(&fHeightMin, "heightMin", "H", 64.0, "image min height")
	flags.Float64VarP(&fRatioMin, "ratioMin", "r", 0.35, "image width/height min value")
	flags.Float64VarP(&fRatioMax, "ratioMax", "R", 2.85, "image width/height max value")
	flags.IntVarP(&fImgNumMin, "imgNumMin", "n", 4, "image num min value which won't be filtered")
	flags.BoolVarP(&fOTrim, "outputTrim", "o", false, "print HTML after trimming")
	flags.StringVarP(&fPicDelim, "delimiter", "d", "\t", "field delimiter")
	flags.IntVarP(&fPicField, "field", "f", 2, "nth field for process, index start from 1")
}

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
	Src                             string
	Top, Left, Width, Height, Ratio float64
}

type ScoredGrp struct {
	Score    int
	ImgItems []ImgItem
}

type ScoredGrpSlice []ScoredGrp

func (sgs ScoredGrpSlice) Len() int {
	return len(sgs)
}

func (sgs ScoredGrpSlice) Swap(i, j int) {
	sgs[i], sgs[j] = sgs[j], sgs[i]
}

// descending order
func (sgs ScoredGrpSlice) Less(i, j int) bool {
	return sgs[j].Score < sgs[i].Score
}

type PicDesc struct {
	OrigLP  string
	LP      string
	Title   string
	SGSlice ScoredGrpSlice
}

type picProcessor struct {
	blackWords []string
	presuffix  string
}

func (w *picProcessor) Map(line []byte) []byte {
	fields := bytes.Split(line, []byte(fPicDelim))
	if len(fields) < fPicField {
		return nil
	}

	var resp CrawlerResp
	if err := json.Unmarshal(fields[fPicField-1], &resp); err != nil {
		return nil
	}
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(resp.HTML))
	if err != nil {
		return nil
	}
	origLP := string(fields[0])
	lp := resp.LandingPage
	picDesc, err := parseDoc(doc, origLP, lp, w.blackWords, w.presuffix)
	if err != nil {
		return nil
	}

	data, err := json.Marshal(picDesc)
	if err != nil {
		return nil
	}
	var buf bytes.Buffer
	buf.WriteString(picDesc.OrigLP)
	buf.Write([]byte{'\t'})
	buf.Write(data)
	buf.Write([]byte{'\n'})

	return buf.Bytes()
}

var PicCmd = &cobra.Command{
	Use:   "pic",
	Short: "Use pictures to describe the webpage.",
	Long: `Check all pictures in the webpage, find the pictures which can best
represent the webpage according to web structure and something else.`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		data, err := conf.FSByte(fEliseDevMode, "/conf/pic.yml")
		if err != nil {
			return err
		}
		viper.SetConfigType("yml")
		err = viper.ReadConfig(bytes.NewReader(data))
		if err != nil {
			return err
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return pic()
	},
}

func pic() error {
	m := &picProcessor{}
	if viper.IsSet("black_words_in_title") {
		m.blackWords = append(m.blackWords, viper.GetStringSlice("black_words_in_title")...)
	}
	if viper.IsSet("post_trim_prefix_suffix") {
		m.presuffix = viper.GetString("post_trim_prefix_suffix")
	}
	fw := fileproc.DummyWrapper()
	if fEliseInPath == "-" {
		return fileproc.ProcTerm(fEliseParallel, fEliseBufMaxSize, m, nil, fw)
	}
	fp := fileproc.NewFileProcessor(fEliseParallel, fEliseBufMaxSize, fEliseSplitCnt, true, false, m, nil, fw)
	err := fp.ProcPath(fEliseInPath, fEliseOutputDir, ".json")
	i, mc, r := fp.Stat()
	log.WithFields(log.Fields{
		"inputLineCnt": i,
		"mapOutCnt":    mc,
		"redOutCnt":    r,
	}).Debug("Finished all work")
	return err
}

func parseDoc(doc *goquery.Document, origLP, lp string, words []string, presuffix string) (*PicDesc, error) {
	origTitle := doc.Find("title").Text()
	title := normalizeTitle(origTitle, words, presuffix)
	if len(title) <= 0 {
		log.WithField("origTitle", origTitle).Debug("Empty title after normalization")
		return nil, errors.New("empty title")
	}

	trimHTML(doc)
	if fOTrim {
		str, _ := doc.Html()
		fmt.Printf("%s\037%s\036\n", lp, gohtml.Format(str))
	}

	tree := extractTree(doc)
	if tree == nil {
		log.Debug("Empty HTML body")
		return nil, errors.New("empty HTML body")
	}

	picDesc := sortTree(tree, lp)
	if picDesc == nil {
		log.Debug("Empty PicDesc")
		return nil, errors.New("Empty PicDesc")
	}

	picDesc.OrigLP = origLP
	picDesc.LP = lp
	picDesc.Title = title
	log.WithField("picDesc", picDesc).Debug("Finished to parse one document")

	return picDesc, nil
}

func normalizeTitle(title string, words []string, presuffix string) string {
	for _, word := range words {
		title = strings.Replace(title, word, "", -1)
	}
	title = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, title)
	title = strings.Trim(title, presuffix)
	return title
}

func trimHTML(doc *goquery.Document) {
	trimNode(doc)
	trimBranch(doc)
}

// trim some node according selector
func trimNode(doc *goquery.Document) {
	for _, selector := range []string{"head", "header", "footer", "aside",
		"script", "noscript", "style", "object", "iframe", "form", "input", "pre", "code",
		"nav", "a", "p", "span", "h1", "h2", "h3", "h4", "h5", "h6", "strong", "em"} {
		doc.Find(selector).Remove()
	}
}

// trim branch which not include img node or unqualified img
func trimBranch(doc *goquery.Document) {
	// only one body node
	sel := doc.Find("body")
	if len(sel.Nodes) == 0 {
		return
	}
	htmlutil.TrimNode(sel.Nodes[0], func(n *html.Node) bool {
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
		// don't trim bad img node before isomorphisim parse
		return false
	})
}

func extractTree(doc *goquery.Document) []*html.Node {
	sel := doc.Find("body")
	if len(sel.Nodes) == 0 {
		// empty HTML body
		return nil
	}

	var tree []*html.Node
	// only one body node
	for _, n := range htmlutil.ExtractIsomorphisms(sel.Nodes[0], leafEqual) {
		tree = append(tree, htmlutil.ExtractIsomorphicLeaf(n, leafEqual)...)
	}
	return tree
}

func leafEqual(c, n *html.Node) bool {
	if c.Data != n.Data {
		return false
	}
	return true
}

func sortTree(tree []*html.Node, lpSrc string) *PicDesc {
	lpURL, err := url.Parse(lpSrc)
	if err != nil {
		log.WithFields(log.Fields{
			"lpSrc": lpSrc,
			"err":   err,
		}).Warn("Failed to parse landing page url")
		return nil
	}
	var sgs ScoredGrpSlice
	for _, n := range tree {
		sg := calcScore(n, lpURL)
		if sg.Score < 1 {
			log.WithField("score", sg.Score).Debug("Score too low")
			continue
		}

		sgs = append(sgs, sg)
	}
	if sgs.Len() < 1 {
		return nil
	}
	sort.Sort(sgs)

	return &PicDesc{SGSlice: sgs}
}

func calcScore(n *html.Node, lpURL *url.URL) ScoredGrp {
	imgItems := extractImg(n, lpURL)
	if len(imgItems) < fImgNumMin {
		log.WithFields(log.Fields{
			"num":    len(imgItems),
			"minNum": fImgNumMin,
		}).Info("Image num under threshold")
		return ScoredGrp{Score: 0}
	}
	return ScoredGrp{Score: len(imgItems), ImgItems: imgItems}
}

//          c--...--img
//         /
// a--..--b--c--...--img
//         \
//          c--...--img
func extractImg(n *html.Node, lpURL *url.URL) []ImgItem {
	for n.FirstChild.Data != "img" && n.FirstChild.NextSibling == nil {
		n = n.FirstChild
	}
	var imgItems []ImgItem
	for curr := n.FirstChild; curr != nil; curr = curr.NextSibling {
		for curr.Data != "img" {
			curr = curr.FirstChild
		}
		img, err := normalizeImg(curr, lpURL)
		if err != nil || filterImg(img) {
			continue
		}
		imgItems = append(imgItems, img)
	}
	length := len(imgItems)
	log.WithField("num", length).Debug("Extract all valid img")

	// remove duplicates
	uniq := make(map[string]bool)
	var totalWidth, totalHeight, totalRatio float64
	for i := 0; i < length; i++ {
		if _, ok := uniq[imgItems[i].Src]; !ok {
			totalWidth += imgItems[i].Width
			totalHeight += imgItems[i].Height
			totalRatio += imgItems[i].Ratio

			uniq[imgItems[i].Src] = true
			continue
		}
		log.WithField("imgSrc", imgItems[i].Src).Info("Filtered by dedup img src")
		imgItems = append(imgItems[:i], imgItems[i+1:]...)
		length--
		i--
	}
	log.WithField("num", length).Debug("After dedup img by src")
	if length <= 2 {
		return imgItems
	}

	// remove item which far away from average
	avgWidth := totalWidth / float64(length)
	avgHeight := totalHeight / float64(length)
	avgRatio := totalRatio / float64(length)
	for i := 0; i < length; i++ {
		img := imgItems[i]
		if !imgOnAverage(img, avgWidth, avgHeight, avgRatio) {
			log.WithFields(log.Fields{
				"img":       img,
				"avgWidth":  avgWidth,
				"avgHeight": avgHeight,
				"avgRatio":  avgRatio,
			}).Info("Filtered by average rect")
			imgItems = append(imgItems[:i], imgItems[i+1:]...)
			length--
			i--
		}
	}

	log.WithField("num", length).Debug("After remove img not on average")
	return imgItems
}

func normalizeImg(n *html.Node, lpURL *url.URL) (ImgItem, error) {
	var imgSrc, lazyImgSrc string
	var img ImgItem
	for _, attr := range n.Attr {
		switch attr.Key {
		case "data-original", "data-src": // suppose they don't coexist
			lazyImgSrc = attr.Val
		case "src":
			imgSrc = attr.Val
		case "prim-top", "prim_top":
			img.Top, _ = strconv.ParseFloat(attr.Val, 64)
		case "prim-left", "prim_left":
			img.Left, _ = strconv.ParseFloat(attr.Val, 64)
		case "prim-width", "prim_width":
			img.Width, _ = strconv.ParseFloat(attr.Val, 64)
		case "prim-height", "prim_height":
			img.Height, _ = strconv.ParseFloat(attr.Val, 64)
		}
	}

	if lazyImgSrc != "" && imgSrc != lazyImgSrc {
		// image not loaded, we may get wrong size
		log.WithFields(log.Fields{
			"lazyImgSrc": lazyImgSrc,
			"imgSrc":     imgSrc,
			"width":      img.Width,
			"height":     img.Height,
		}).Debug("Ignore image which was not loaded")
		return img, errors.New("ignore image which was not loaded")
	}
	if imgSrc == "" {
		var buf bytes.Buffer
		html.Render(&buf, n)
		log.WithFields(log.Fields{
			"node": n,
			"HTML": buf.String(),
		}).Debug("Can't find img src")
		return img, errors.New("can't find img src")
	}

	imgURL, err := url.Parse(imgSrc)
	if err != nil {
		log.WithFields(log.Fields{
			"imgSrc": imgSrc,
			"err":    err,
		}).Debug("Failed to parse img url")
		return img, errors.New("failed to parse img url")
	}

	img.Src = lpURL.ResolveReference(imgURL).String()
	img.Ratio = img.Width / img.Height
	log.WithField("imgItem", img).Debug("Normalize image")

	return img, nil
}

func filterImg(img ImgItem) bool {
	return filterImgbyRect(img) ||
		filterImgbyExt(img)
}

func filterImgbyRect(img ImgItem) bool {
	width, height, ratio := img.Width, img.Height, img.Ratio
	log.WithFields(log.Fields{
		"width":  width,
		"height": height,
		"ratio":  ratio,
	}).Debug("Get img rect")
	if width < fWidthMin || height < fHeightMin {
		log.WithFields(log.Fields{
			"width":     width,
			"height":    height,
			"minWidth":  fWidthMin,
			"minHeight": fHeightMin,
		}).Info("Filtered by width or height")
		return true
	}
	if ratio < fRatioMin || ratio > fRatioMax {
		log.WithFields(log.Fields{
			"width":    width,
			"height":   height,
			"ratio":    ratio,
			"minRatio": fRatioMin,
			"maxRatio": fRatioMax,
		}).Info("Filtered by width/height ratio")
		return true
	}

	return false
}

func filterImgbyExt(img ImgItem) bool {
	imgSrc := img.Src
	if imgSrc == "" {
		log.Warn("Can't find img src while filtering")
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

func imgOnAverage(img ImgItem, avgWidth, avgHeight, avgRatio float64) bool {
	ratio := img.Ratio
	if ratio == avgRatio {
		return true
	} else if math.Abs(ratio-avgRatio)/avgRatio > 0.1 {
		return false
	}

	width := img.Width
	height := img.Height
	if math.Abs(width-avgWidth)/avgWidth > 0.1 || math.Abs(height-avgHeight)/avgHeight > 0.1 {
		return false
	}

	return true
}

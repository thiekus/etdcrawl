package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/beevik/etree"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

/* Our metadata store in JSON */
type DocumentInfo struct {
	DocumentId string `json:"documentId"`
	Title      string `json:"title"`
	Author     string `json:"author"`
	DateTime   string `json:"dateTime"`
	Abstract   string `json:"abstract"`
	Document   string `json:"document"`
}

const (
	baseIndexUrl        = "https://etd.unsyiah.ac.id/index.php"
	baseIndexUrlDetails = "://etd.unsyiah.ac.id/index.php?p=show_detail&"
	baseRepositoryUrl   = "https://etd.unsyiah.ac.id/repository/"
)

var (
	outDir      = "./"
	embargoFlag = 0
	pageIndex   = 1
	maxPage     = 0xffffffff
	minId       = 0
	maxId       = 0xffffffff
	withPdf     = true
	ignoreCert  = false
)

var (
	crawlCount    = 0
	crawlCountMtx sync.Mutex
	crawlDone     = false
	crawlFetched  = make(map[string]bool) /* To avoid duplicate crawl */
	crawlWg       sync.WaitGroup
)

func isFileExists(filePath string) bool {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return false
	}
	return true
}

func fetchData(urlPath string) ([]byte, error) {
	log.Printf("Fetching from %s", urlPath)
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: ignoreCert},
	}
	client := &http.Client{Transport: tr}
	resp, err := client.Get(urlPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, errors.New("HTTP error " + strconv.Itoa(resp.StatusCode))
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func getDocumentIdFromUrl(urlPath string) (string, bool) {
	if strings.Contains(urlPath, baseIndexUrlDetails) {
		urlParse, err := url.Parse(urlPath)
		if err != nil {
			return "", false
		}
		id := urlParse.Query().Get("id")
		if _, exists := crawlFetched[id]; exists {
			return "", false
		} else {
			crawlFetched[id] = true
			/* Don't repeat yourself */
			if isFileExists(outDir + id + ".json") {
				return "", false
			}
			return id, true
		}
	} else {
		return "", false
	}
}

func parseIndexPage(pageData []byte) ([]string, error) {
	r := bytes.NewReader(pageData)
	doc, err := goquery.NewDocumentFromReader(r)
	if err != nil {
		return nil, err
	}
	var urls []string = nil
	/* Iterate all entries in index, but limit only that *exists* in indexes! */
	doc.Find("table.zebra-table").Each(func(i int, sel *goquery.Selection) {
		/* Avoid abstract link, specific only to article details */
		sel.Find("td a").Each(func(j int, sel *goquery.Selection) {
			if href, exists := sel.Attr("href"); exists {
				/* eg: http://etd.unsyiah.ac.id/index.php?p=show_detail&id=14278 */
				if id, exists := getDocumentIdFromUrl(href); exists {
					urls = append(urls, id)
				}
			}
		})
	})
	return urls, nil
}

func crawlDocument(docId string) {
	go func(docId string) {
		crawlWg.Add(1)
		defer crawlWg.Done()
		/* Fetch Slims metadata */
		metadataUrl := "https://etd.unsyiah.ac.id/index.php?p=show_detail&inXML=true&id=" + docId
		data, err := fetchData(metadataUrl)
		if err != nil {
			log.Printf("ERROR: Cannot fetch metadata for docId %s: %s", docId, err)
			return
		}
		doc := etree.NewDocument()
		if err := doc.ReadFromBytes(data); err != nil {
			log.Printf("ERROR: Cannot parse metadata for docId %s: %s", docId, err)
			return
		}
		/* Retrieve xml metadata and store at json struct */
		rootXml := doc.SelectElement("modsCollection").SelectElement("mods")
		title := ""
		if titleInfo := rootXml.SelectElement("titleInfo"); titleInfo != nil {
			title = titleInfo.SelectElement("title").Text()
		}
		author := ""
		if name := rootXml.SelectElement("name"); name != nil {
			author = name.SelectElement("namePart").Text()
		}
		abstract := ""
		if note := rootXml.SelectElement("note"); note != nil {
			abstract = note.Text()
		}
		dateTime := ""
		if recordInfo := rootXml.SelectElement("recordInfo"); recordInfo != nil {
			dateTime = recordInfo.SelectElement("recordCreationDate").Text()
		}
		documentName := ""
		if slimsDigital := rootXml.SelectElement("slims_digitals"); slimsDigital != nil {
			documentName = strings.ReplaceAll(slimsDigital.SelectElement("slims_digital_item").SelectAttr("path").Value, "/", "")
		}
		metadata := DocumentInfo{
			DocumentId: docId,
			Title:      title,
			Author:     author,
			DateTime:   dateTime,
			Abstract:   abstract,
			Document:   documentName,
		}
		/* Fetch real document, if available */
		if withPdf && (documentName != "") {
			pdf, err := fetchData(baseRepositoryUrl + documentName)
			if err != nil {
				log.Printf("ERROR: Cannot fetch PDF for docId %s: %s", docId, err)
				return
			}
			if err := ioutil.WriteFile(outDir+documentName, pdf, os.ModePerm); err != nil {
				log.Printf("ERROR: Cannot write %s: %s", documentName, err)
				return
			}
		}
		/* If succeeded, save also metadata */
		if jsonData, err := json.Marshal(metadata); err == nil {
			if err = ioutil.WriteFile(outDir+docId+".json", jsonData, os.ModePerm); err == nil {
				log.Printf("Document %s saved!", docId)
				crawlCountMtx.Lock()
				crawlCount++
				crawlCountMtx.Unlock()
			} else {
				log.Printf("ERROR: Cannot save metadata for %s, %s", docId, err)
			}
		} else {
			log.Printf("ERROR: Cannot marshall metadata for %s, %s", docId, err)
		}
	}(docId)
}

func main() {
	fmt.Println("Unsyiah ETD crawler, codename Cosmos")
	fmt.Println("Copyright (C) Thiekus 2019")
	fmt.Println("")
	/* Commandline flags */
	outDirPtr := flag.String("outdir", "", "Output directory")
	embargoFlagPtr := flag.Int("embargo", embargoFlag, "Embargo flag (0 for fulltext)")
	pageIndexPtr := flag.Int("page", pageIndex, "Page number index start")
	maxPagePtr := flag.Int("maxpage", maxPage, "Page number maximum")
	minIdPtr := flag.Int("min", minId, "Minimum content id (default 0)")
	maxIdPtr := flag.Int("max", maxId, "Maximum content id")
	withPdfPtr := flag.Bool("pdf", withPdf, "Fetch with full PDF document")
	ignoreCertPtr := flag.Bool("ignorecert", ignoreCert, "Ignore TLS certificate errors")
	/* Don't forget to parse */
	flag.Parse()
	/* Assign to global variables */
	outDir = *outDirPtr
	if outDir == "" {
		log.Print("Error: output directory not specified!")
		return
	}
	embargoFlag = *embargoFlagPtr
	pageIndex = *pageIndexPtr
	maxPage = *maxPagePtr
	minId = *minIdPtr
	maxId = *maxIdPtr
	withPdf = *withPdfPtr
	ignoreCert = *ignoreCertPtr
	/* CTRL+C Interrupt handler */
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Print("Caught in interrupt!")
		crawlDone = true
	}()
	/* Do crawling */
	crawlDone = false
	urlIndexBase, err := url.Parse(baseIndexUrl)
	if err != nil {
		log.Fatal(err)
	}
	q := urlIndexBase.Query()
	q.Add("embargo", strconv.Itoa(embargoFlag))
	for idx := pageIndex; idx <= maxPage; idx++ {
		if crawlDone {
			break
		}
		q.Set("page", strconv.Itoa(idx))
		urlIndexBase.RawQuery = q.Encode()
		urlIndex := urlIndexBase.String()
		indexData, err := fetchData(urlIndex)
		if err != nil {
			log.Printf("WARNING: Fetch %s error: %s", urlIndex, err)
		} else {
			urls, err := parseIndexPage(indexData)
			if err != nil {
				log.Printf("WARNING: Parse %s error: %s", urlIndex, err)
			} else {
				/* Have fetched before but indexes are empty now */
				if (urls == nil) && (crawlCount > 0) {
					log.Print("No more document in index page")
					break
				} else {
					/* Iterate and fetch pending documents */
					for i := 0; i < len(urls); i++ {
						crawlDocument(urls[i])
					}
				}
			}
		}
		/* We hate corrupt result, be patient until all done */
		log.Print("Waiting for pending routines...")
		crawlWg.Wait()
	}
	log.Printf("Done, %d documents was fetched", crawlCount)
}

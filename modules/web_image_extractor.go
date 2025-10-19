package modules

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/models"
	"github.com/PuerkitoBio/goquery"
)

var httpClient = &http.Client{
	Timeout: 8 * time.Second,
	Transport: &http.Transport{
		ForceAttemptHTTP2: false, // Force HTTP/1.1
	},
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return http.ErrUseLastResponse
		}
		return nil
	},
}

func ExtractMetadata(url string) (*models.Web_Image_MetaData, error) {
	//start := time.Now()

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch URL, status: %d", resp.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML: %v", err)
	}

	data := &models.Web_Image_MetaData{}
	if desc, exists := doc.Find(`meta[name="description"]`).Attr("content"); exists {
		data.Description = strings.TrimSpace(desc)
	} else if ogDesc, exists := doc.Find(`meta[property="og:description"]`).Attr("content"); exists {
		data.Description = strings.TrimSpace(ogDesc)
	}

	if icon, exists := doc.Find(`link[rel="icon"]`).Attr("href"); exists {
		data.Favicon = resolveURL(url, icon)
	} else if shortcut, exists := doc.Find(`link[rel="shortcut icon"]`).Attr("href"); exists {
		data.Favicon = resolveURL(url, shortcut)
	} else {
		data.Favicon = fmt.Sprintf("https://t3.gstatic.com/faviconV2?client=SOCIAL&type=FAVICON&url=%s&size=32", url)
		fmt.Println("hitting our default scrapper-->", data.Favicon)
	}
	//fmt.Println("data.Favicon-->", data.Favicon)
	return data, nil
}

func resolveURL(base, ref string) string {
	if strings.HasPrefix(ref, "http") {
		return ref
	}
	if strings.HasPrefix(ref, "//") {
		return "https:" + ref
	}
	if strings.HasPrefix(ref, "/") {
		if u, err := http.NewRequest("GET", base, nil); err == nil {
			return u.URL.Scheme + "://" + u.URL.Host + ref
		}
	}
	return ref
}

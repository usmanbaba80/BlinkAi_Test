package modules

import (
	"fmt"
	"net/http"
	"net/url"
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

func ExtractMetadata(url1 string) (*models.Web_Image_MetaData, error) {
	//start := time.Now()

	req, _ := http.NewRequest("GET", url1, nil)
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
	// 🧠 Extract website name
	if siteName, exists := doc.Find(`meta[property="og:site_name"]`).Attr("content"); exists {
		data.WebSource = strings.TrimSpace(siteName)
		fmt.Println("using the og:site_name as the name-->", data.WebSource)
		// } else if title := doc.Find("title").Text(); title != "" {
		// 	data.Name = strings.TrimSpace(title)
		// 	fmt.Println("using the tile as the name-->", data.Name)
	} else {
		// Fallback: extract from domain name
		data.WebSource = webSourceFromUrl(url1)
	}
	if desc, exists := doc.Find(`meta[name="description"]`).Attr("content"); exists {
		data.Description = strings.TrimSpace(desc)
	} else if ogDesc, exists := doc.Find(`meta[property="og:description"]`).Attr("content"); exists {
		data.Description = strings.TrimSpace(ogDesc)
	}

	if icon, exists := doc.Find(`link[rel="icon"]`).Attr("href"); exists {
		data.Favicon = resolveURL(url1, icon)
	} else if shortcut, exists := doc.Find(`link[rel="shortcut icon"]`).Attr("href"); exists {
		data.Favicon = resolveURL(url1, shortcut)
	} else {
		data.Favicon = fmt.Sprintf("https://t3.gstatic.com/faviconV2?client=SOCIAL&type=FAVICON&url=%s&size=32", url1)
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
func webSourceFromUrl(url1 string) string {
	parsedURL, err := url.Parse(url1)
	if err == nil {
		host := parsedURL.Hostname()
		host = strings.Replace(host, "www.", "", 1)
		host = strings.Replace(host, "m.", "", 1)
		host = strings.Replace(host, "edition.", "", 1)
		parts := strings.Split(host, ".")
		return parts[len(parts)-2]
	}
	return ""
}

package modules

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ExtractYouTubeID extracts a video ID from common YouTube URL formats.
func ExtractYouTubeID(rawURL string) (string, bool) {
	// Examples:
	// https://www.youtube.com/watch?v=VIDEOID
	// https://youtu.be/VIDEOID
	// https://www.youtube.com/embed/VIDEOID
	if rawURL == "" {
		return "", false
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", false
	}
	// watch?v=
	if strings.Contains(u.Host, "youtube.com") {
		q := u.Query().Get("v")
		if q != "" {
			return q, true
		}
		// /embed/VIDEOID
		parts := strings.Split(strings.Trim(u.Path, "/"), "/")
		for i := 0; i < len(parts)-1; i++ {
			if parts[i] == "embed" && i+1 < len(parts) {
				return parts[i+1], true
			}
		}
	}
	// youtu.be/VIDEOID
	if strings.Contains(u.Host, "youtu.be") {
		id := strings.Trim(strings.Trim(u.Path, "/"), " ")
		if id != "" {
			return id, true
		}
	}
	return "", false
}

// parseCountString converts strings like "329,067,736 views", "1.2M", "45K" to an integer.
func parseCountString(s string) int {
	s = strings.ToUpper(strings.TrimSpace(s))
	// remove words like "VIEWS", "LIKES"
	s = strings.ReplaceAll(s, "VIEWS", "")
	s = strings.ReplaceAll(s, "LIKES", "")
	s = strings.TrimSpace(s)
	// handle K/M/B suffixes
	mult := 1.0
	if strings.HasSuffix(s, "K") {
		mult = 1_000
		s = strings.TrimSuffix(s, "K")
	} else if strings.HasSuffix(s, "M") {
		mult = 1_000_000
		s = strings.TrimSuffix(s, "M")
	} else if strings.HasSuffix(s, "B") {
		mult = 1_000_000_000
		s = strings.TrimSuffix(s, "B")
	}
	// remove commas and spaces
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int(f * mult)
	}
	// fallback: extract digits
	digits := regexp.MustCompile(`\d+`).FindAllString(s, -1)
	if len(digits) == 0 {
		return 0
	}
	joined := strings.Join(digits, "")
	if n, err := strconv.ParseInt(joined, 10, 64); err == nil {
		return int(n)
	}
	return 0
}

type scrapingDogVideo struct {
	ID            string `json:"id"`
	Title         string `json:"title"`
	LengthSeconds int    `json:"length_seconds"`
	Views         string `json:"views"`
	Likes         string `json:"likes"`
	Author        string `json:"author"`
	PublishedTime string `json:"published_time"`
	Description   string `json:"description"`
	Thumbnail     string `json:"thumbnail"`
}

type scrapingDogResponse struct {
	Video scrapingDogVideo `json:"video"`
}

// FetchYouTubeDetails calls ScrapingDog YouTube API and returns the parsed response.
func FetchYouTubeDetails(ctx context.Context, videoID string) (*scrapingDogResponse, error) {
	apiKey := os.Getenv("Scrapping_Dog_API_Key")
	if apiKey == "" {
		return nil, fmt.Errorf("Scrapping_Dog_API_Key missing")
	}
	endpoint := "https://api.scrapingdog.com/youtube/video"
	q := url.Values{}
	q.Set("api_key", apiKey)
	q.Set("v", videoID)
	urlStr := endpoint + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("scrapingdog status %d", resp.StatusCode)
	}
	var out scrapingDogResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// MapScrapingDogToVideoFields maps the ScrapingDog response to our video fields.
type YouTubeEnrichment struct {
	VideoID         string
	Title           string
	ThumbnailURL    string
	ThumbnailWidth  int
	ThumbnailHeight int
	PublishDate     string
	Likes           int
	Views           int
	Description     string
}

func BuildEnrichment(sd *scrapingDogResponse) YouTubeEnrichment {
	if sd == nil {
		return YouTubeEnrichment{}
	}
	v := sd.Video
	return YouTubeEnrichment{
		VideoID:         v.ID,
		Title:           v.Title,
		ThumbnailURL:    v.Thumbnail,
		ThumbnailWidth:  0, // unknown from API; keep existing if present
		ThumbnailHeight: 0, // unknown from API; keep existing if present
		PublishDate:     v.PublishedTime,
		Likes:           parseCountString(v.Likes),
		Views:           parseCountString(v.Views),
		Description:     v.Description,
	}
}

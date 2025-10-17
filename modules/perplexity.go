package modules

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/database"
	//"github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/models"
	importModel "github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

// helpers to choose non-zero enrichment values
func chooseString(a string, fallback string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return fallback
}

func chooseInt(a int, fallback int) int {
	if a != 0 {
		return a
	}
	return fallback
}

// // wrapped update structure to preserve JSON field order
// type videoUpdate struct {
// 	SeqNo      int                   `json:"seq_no"`
// 	IsCitation bool                  `json:"is_citation"`
// 	Kind       string                `json:"kind"`
// 	Payload    importModel.VideoItem `json:"payload"`
// }

const perplexityURL = "https://api.perplexity.ai/chat/completions"

// AppendContentToFile appends content lines to a per-request text file identified by searchID.
// The file is created on first write in a dedicated directory.
func AppendContentToFile(searchID, content string) error {
	if strings.TrimSpace(searchID) == "" || strings.TrimSpace(content) == "" {
		return nil
	}
	dir := "ai_content"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := fmt.Sprintf("%s/%s.txt", dir, searchID)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	// Write a newline-terminated content chunk
	if _, err := f.WriteString(content + "\n"); err != nil {
		return err
	}
	return nil
}

//var combined_global = make([]importModel.CombinedItem, 0, 30)

// Use types from models to avoid duplication

// tcp broadcaster (singleton) — accepts TCP clients and broadcasts chunk lines to all
// var (
// 	tcpOnce              sync.Once
// 	globalTCPBroadcaster *tcpBroadcaster
// )

// type tcpBroadcaster struct {
// 	addr  string
// 	ln    net.Listener
// 	mu    sync.Mutex
// 	conns map[net.Conn]struct{}
// 	msgCh chan string
// }

// func (b *tcpBroadcaster) start() error {
// 	ln, err := net.Listen("tcp", b.addr)
// 	if err != nil {
// 		return err
// 	}
// 	b.ln = ln
// 	b.conns = make(map[net.Conn]struct{})
// 	// accept loop
// 	go func() {
// 		for {
// 			conn, err := b.ln.Accept()
// 			if err != nil {
// 				return
// 			}
// 			b.mu.Lock()
// 			b.conns[conn] = struct{}{}
// 			b.mu.Unlock()
// 		}
// 	}()
// 	// broadcast loop
// 	go func() {
// 		for msg := range b.msgCh {
// 			b.mu.Lock()
// 			for c := range b.conns {
// 				if _, err := io.WriteString(c, msg); err != nil {
// 					c.Close()
// 					delete(b.conns, c)
// 				}
// 			}
// 			b.mu.Unlock()
// 		}
// 	}()
// 	return nil
// }

// func ensureTCPBroadcaster(addr string) {
// 	tcpOnce.Do(func() {
// 		b := &tcpBroadcaster{addr: addr, msgCh: make(chan string, 1024)}
// 		if err := b.start(); err != nil {
// 			fmt.Printf("tcp broadcaster error: %v\n", err)
// 			return
// 		}
// 		globalTCPBroadcaster = b
// 	})
// }

// func broadcastTCPLine(line string) {
// 	if globalTCPBroadcaster == nil {
// 		return
// 	}
// 	select {
// 	case globalTCPBroadcaster.msgCh <- line:
// 	default:
// 		// drop if buffer full to avoid blocking streaming path
// 	}
// }

// Per-client asynchronous registry-based socket system
type ClientConnection struct {
	SearchID string
	Conn     net.Conn
	MsgCh    chan string
	mu       sync.Mutex
	closed   bool
}

var (
	clientRegistry     = make(map[string]*ClientConnection)
	clientRegistryLock sync.RWMutex
)

// Start TCP listener (once)
func ensureTCPServer(addr string) {
	go func() {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			fmt.Println("❌ TCP listen error:", err)
			return
		}
		fmt.Println("🚀 TCP server listening on", addr)
		for {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Println("⚠️ Accept error:", err)
				continue
			}
			remote := conn.RemoteAddr().String()
			fmt.Println("🔌 TCP client connected from", remote)
			go handleClient(conn)
		}
	}()
}

// EnsureTCPServer is an exported helper to start the TCP server from other packages (e.g., at app startup).
func EnsureTCPServer(addr string) {
	ensureTCPServer(addr)
}

func handleClient(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Wait for SearchID
	searchID, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("⚠️ Failed to read SearchID:", err)
		return
	}
	searchID = strings.TrimSpace(searchID)
	fmt.Println("🔗 Registered client:", searchID, "from", conn.RemoteAddr().String())

	client := &ClientConnection{
		SearchID: searchID,
		Conn:     conn,
		MsgCh:    make(chan string, 100),
	}

	clientRegistryLock.Lock()
	clientRegistry[searchID] = client
	clientRegistryLock.Unlock()

	go streamToClient(client)

	select {} // Keep the goroutine alive
}

func streamToClient(client *ClientConnection) {

	for msg := range client.MsgCh {
		// if len(client.MsgCh) == cap(client.MsgCh) {
		// 	fmt.Printf("list socket backlog full for %s: %d/%d; dropping newest\n", client.SearchID, len(client.MsgCh), cap(client.MsgCh))
		// }
		// fmt.Printf("queue depth for %s: %d/%d\n", client.SearchID, len(client.MsgCh), cap(client.MsgCh))

		// Debug print suppressed
		_, err := client.Conn.Write([]byte(msg))
		if err != nil {
			fmt.Println("❌ Write error for", client.SearchID, ":", err)
			break
		}
	}
	clientRegistryLock.Lock()
	delete(clientRegistry, client.SearchID)
	clientRegistryLock.Unlock()
	fmt.Println("🔌 Client disconnected:", client.SearchID)
}

func sendToClient(searchID, chunk string) {
	clientRegistryLock.RLock()
	client, ok := clientRegistry[searchID]
	clientRegistryLock.RUnlock()
	if !ok {
		fmt.Println("ℹ️ No registered client for searchID:", searchID, "— skipping send")
		return
	}
	// Serialize with client close to avoid sending on a closed channel
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return
	}
	select {
	case client.MsgCh <- chunk:
		// sent
	default:
		// drop if full
	}
	client.mu.Unlock()
	//_ = AppendContentToFile(searchID, "newChunk---->"+chunk)
}

// WaitForClientRegistered blocks until a client with the given searchID is registered
// or the timeout elapses. Returns true if registered within the given timeout.
func WaitForClientRegistered(searchID string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		clientRegistryLock.RLock()
		_, ok := clientRegistry[searchID]
		clientRegistryLock.RUnlock()
		if ok {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// DisconnectClient closes the client's channel and TCP connection, and removes it from the registry.
func DisconnectClient(searchID string) {
	clientRegistryLock.Lock()
	client, ok := clientRegistry[searchID]
	if ok {
		delete(clientRegistry, searchID)
	}
	clientRegistryLock.Unlock()
	if !ok || client == nil {
		fmt.Println("ℹ️ Disconnect requested, but no client found for:", searchID)
		return
	}
	fmt.Println("👋 Gracefully closing client:", searchID)
	// Guard close with client mutex to avoid concurrent sends panicking
	client.mu.Lock()
	if !client.closed {
		client.closed = true
		close(client.MsgCh)
	}
	client.mu.Unlock()
	_ = client.Conn.Close()
}

// StreamPerplexity issues the HTTP request and returns the streaming response body.
// Caller is responsible for closing the body.
func StreamPerplexity(ctx context.Context, client *http.Client, apiKey string, reqBody importModel.PerplexityRequest) (io.ReadCloser, *http.Response, error) {
	if client == nil {
		client = &http.Client{Timeout: 0}
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, perplexityURL, strings.NewReader(string(payload)))
	if err != nil {
		return nil, nil, fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("do request: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, resp, fmt.Errorf("perplexity status %d: %s", resp.StatusCode, string(b))
	}
	return resp.Body, resp, nil
}

// ForwardStream reads SSE lines from r, logs object and content via logFn, and writes raw lines to w as SSE.
// It extracts content either from choices[0].message.content or choices[0].delta.content if present.
func ForwardStream(ctx context.Context, r io.Reader, w io.Writer, logFn func(object string, content string), search_id_ai_summary, search_id_list string, pool *pgxpool.Pool, searchRecord *importModel.Search, platoform string) error {
	// Ensure SSE headers were already set by the caller.
	// Ensure TCP broadcaster is up to stream chunk lines to TCP clients.
	//ensureTCPBroadcaster(":9000")
	//ensureTCPServer(":9000")

	reader := bufio.NewReader(r)
	enc := json.NewEncoder(w)

	// We wrap each piece we print as minimal JSON to the client as an SSE data: line.
	writeSSE := func(data any) error {
		// Format as: data: <json>\n\n
		var sb strings.Builder
		sb.WriteString("data: ")
		b, _ := json.Marshal(data)
		sb.Write(b)
		sb.WriteString("\n\n")
		if _, err := io.WriteString(w, sb.String()); err != nil {
			return err
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		return nil
	}

	_ = enc // keep for future expansions; currently not used directly
	combined_global := make([]importModel.CombinedItem, 0, 30)
	var wg sync.WaitGroup
	firstListPrinted := false
	lastNonEmptyContent := ""
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// SSE lines generally start with "data: ". Ignore non-data lines.
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		jsonPart := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if jsonPart == "[DONE]" {
			// end of stream
			return nil
		}
		var chunk importModel.PerplexityChunk
		if err := json.Unmarshal([]byte(jsonPart), &chunk); err != nil {
			// If chunk JSON fails, still forward raw line to client to avoid breaking stream
			_ = writeSSE(map[string]string{"raw": jsonPart})
			continue
		}
		content := ""
		if len(chunk.Choices) > 0 {
			if chunk.Choices[0].Message != nil {
				content = chunk.Choices[0].Message.Content
			} else if chunk.Choices[0].Delta != nil {
				content = chunk.Choices[0].Delta.Content
			}
		}
		if !firstListPrinted {
			firstListPrinted = true
			// Build and print the initial combined list in a background goroutine
			go func(c importModel.PerplexityChunk) {
				// Build typed combined list from search_results, images, videos
				seq := 1
				// Per-kind seen sets to avoid duplicates
				normalize := func(u string) string {
					u = strings.TrimSpace(u)
					u = strings.ToLower(u)
					// drop trailing slash for stability
					for strings.HasSuffix(u, "/") {
						u = strings.TrimSuffix(u, "/")
					}
					return u
				}
				seenVideo := make(map[string]struct{}, 64)
				seenWeb := make(map[string]struct{}, 64)
				seenImage := make(map[string]struct{}, 64)
				//combined := make([]importModel.CombinedItem, 0, 30)
				// search_results classified as video (YouTube) or web; both are citations
				for _, sr := range c.SearchResults {

					isYouTube := strings.Contains(sr.URL, "youtube.com") || strings.Contains(sr.URL, "youtu.be")
					if isYouTube {
						// Skip YouTube channel and shorts URLs
						if strings.Contains(sr.URL, "/channel/") || strings.Contains(sr.URL, "/shorts/") {
							continue
						}
						key := normalize(sr.URL)
						if _, dup := seenVideo[key]; dup {
							continue
						}
						seenVideo[key] = struct{}{}
						combined_global = append(combined_global, importModel.CombinedItem{
							SeqNo:      seq,
							IsCitation: true,
							Kind:       "video",
							Payload: importModel.VideoItem{
								URL:             sr.URL,
								Title:           sr.Title,
								ThumbnailURL:    "",
								ThumbnailWidth:  0,
								ThumbnailHeight: 0,
								VideoID:         "",
								PublishDate:     sr.Date,
								Likes:           0,
								Views:           0,
								Description:     sr.Snippet,
							},
						})
						seq++
						continue
					}
					// Dedupe web by URL
					wkey := normalize(sr.URL)
					if _, dup := seenWeb[wkey]; dup {
						seq++
						continue
					}
					seenWeb[wkey] = struct{}{}
					combined_global = append(combined_global, importModel.CombinedItem{
						SeqNo:      seq,
						IsCitation: true,
						Kind:       "web",
						Payload: importModel.WebItem{
							Title:       sr.Title,
							URL:         sr.URL,
							Date:        sr.Date,
							LastUpdated: sr.LastUpdated,
							Snippet:     sr.Snippet,
							Source:      sr.Source,
							Favicon:     "",
							AIOverview:  "",
						},
					})
					seq++
				}
				// images (not citations)
				for _, im := range c.Images {
					// Dedupe images by OriginURL if present, else ImageURL
					ikey := im.OriginURL
					if strings.TrimSpace(ikey) == "" {
						ikey = im.ImageURL
					}
					ikey = normalize(ikey)
					if _, dup := seenImage[ikey]; dup {
						seq++
						continue
					}
					seenImage[ikey] = struct{}{}
					combined_global = append(combined_global, importModel.CombinedItem{
						SeqNo:      seq,
						IsCitation: false,
						Kind:       "image",
						Payload: &importModel.ImageItem{
							ImageURL:   im.ImageURL,
							OriginURL:  im.OriginURL,
							Height:     im.Height,
							Width:      im.Width,
							Title:      im.Title,
							Snippet:    "",
							Favicon:    "",
							Source:     "",
							AIOverview: "",
						},
					})
					seq++
				}
				// videos (not citations)
				for _, v := range c.Videos {
					// Skip YouTube channel and shorts URLs
					if strings.Contains(v.URL, "/channel/") || strings.Contains(v.URL, "/shorts/") {
						continue
					}
					key := normalize(v.URL)
					if _, dup := seenVideo[key]; dup {
						seq++
						continue
					}
					seenVideo[key] = struct{}{}
					combined_global = append(combined_global, importModel.CombinedItem{
						SeqNo:      seq,
						IsCitation: false,
						Kind:       "video",
						Payload: importModel.VideoItem{
							URL:             v.URL,
							ThumbnailWidth:  v.ThumbnailWidth,
							ThumbnailHeight: v.ThumbnailHeight,
							ThumbnailURL:    v.ThumbnailURL,
						},
					})
					seq++
				}
				//here at the end of the list also add the related search means it will also happens at the end of the list
				// related questions (append as one grouped item at end)
				if len(c.RelatedQuestions) > 0 {
					combined_global = append(combined_global, importModel.CombinedItem{
						SeqNo:      seq,
						IsCitation: false,
						Kind:       "related",
						Payload: importModel.RelatedList{
							Queries: c.RelatedQuestions,
						},
					})
					seq++
				}

				// snapshot to global for later access (e.g., at stop)
				//combined_global = combined_global
				// Print combined list once (pretty printed for readability)
				if b, err := json.MarshalIndent(combined_global, "", "  "); err == nil {
					//fmt.Printf("combined_list:\n%s\n", string(b))
					if search_id_list != "" {
						sendToClient(search_id_list, string(b)+search_id_list+"\n")
					}
				}
				// first socket can be inistiated here to send data to extractors (non-blocking)
				// Enrich YouTube video items asynchronously via ScrapingDog

				for i := range combined_global {
					// v, ok := combined_global[i].Payload.(importModel.VideoItem)
					// if !ok {
					// 	continue
					// }
					// if v.URL == "" || !(strings.Contains(v.URL, "youtube.com") || strings.Contains(v.URL, "youtu.be")) {
					// 	continue
					// }
					idx := i
					urlStr := ""
					if combined_global[idx].Kind == "web" {
						switch p := combined_global[idx].Payload.(type) {
						case importModel.WebItem:
							urlStr = p.URL
						case *importModel.WebItem:
							if p != nil {
								urlStr = p.URL
							}
						}
					} else if combined_global[idx].Kind == "image" {
						if p, ok := combined_global[idx].Payload.(*importModel.ImageItem); ok && p != nil {
							urlStr = p.ImageURL
						}
					} else if combined_global[idx].Kind == "video" {
						if p, ok := combined_global[idx].Payload.(importModel.VideoItem); ok {
							urlStr = p.URL
						}
					}

					if combined_global[idx].Kind == "related" {
						continue
					}
					if combined_global[idx].Kind == "image" {
						wg.Add(1)
						go func() {
							defer wg.Done()
							p, ok := combined_global[idx].Payload.(*importModel.ImageItem)
							if !ok || p == nil {
								return
							}
							data, err := ExtractMetadata(urlStr)
							if err != nil {
								fmt.Printf("ExtractMetadata error for seq_no=%d url=%s: %v\n", combined_global[idx].SeqNo, urlStr, err)
							}
							snippet := ""
							favicon := ""
							if err == nil && data != nil {
								snippet = data.Description
								favicon = data.Favicon
							}
							newPayload := importModel.ImageItem{
								ImageURL:   p.ImageURL,
								OriginURL:  p.OriginURL,
								Height:     p.Height,
								Width:      p.Width,
								Title:      p.Title,
								Snippet:    snippet,
								Favicon:    favicon,
								Source:     p.Source,
								AIOverview: p.AIOverview,
							}
							// if b, err := json.MarshalIndent(importModel.CombinedItem{
							// 	SeqNo:      combined_global[idx].SeqNo,
							// 	IsCitation: combined_global[idx].IsCitation,
							// 	Kind:       "image",
							// 	Payload:    newPayload,
							// }, "", "  "); err == nil {
							// 	fmt.Printf("enriched_image_item:\n%s\n", string(b))
							// }
							if search_id_list != "" {
								if b, err := json.Marshal(importModel.CombinedItem{
									SeqNo:      combined_global[idx].SeqNo,
									IsCitation: combined_global[idx].IsCitation,
									Kind:       "image",
									Payload:    newPayload,
								}); err == nil {
									combined_global[idx].Payload = newPayload
									sendToClient(search_id_list, string(b)+search_id_list+"\n")
								}
							}
						}()
						continue
					}
					if combined_global[idx].Kind == "web" {
						wg.Add(1)
						go func() {
							defer wg.Done()
							//call the web_image_extractor
							data, err := ExtractMetadata(urlStr)
							if err != nil {
								fmt.Printf("ExtractMetadata error for seq_no=%d url=%s: %v\n", combined_global[idx].SeqNo, urlStr, err)
							}
							var current importModel.WebItem
							switch p := combined_global[idx].Payload.(type) {
							case importModel.WebItem:
								current = p
							case *importModel.WebItem:
								if p == nil {
									return
								}
								current = *p
							default:
								return
							}
							// derive fields safely from extractor result
							newSnippet := current.Snippet
							newFavicon := current.Favicon
							if err == nil && data != nil {
								if strings.TrimSpace(data.Description) != "" {
									newSnippet = data.Description
								}
								if strings.TrimSpace(data.Favicon) != "" {
									newFavicon = data.Favicon
								}
							}
							newPayload := importModel.WebItem{
								Title:       current.Title,
								URL:         current.URL,
								Date:        current.Date,
								LastUpdated: current.LastUpdated,
								Snippet:     newSnippet,
								Source:      current.Source,
								Favicon:     newFavicon,
								AIOverview:  current.AIOverview,
							}
							// if b, err := json.MarshalIndent(importModel.CombinedItem{
							// 	SeqNo:      combined_global[idx].SeqNo,
							// 	IsCitation: combined_global[idx].IsCitation,
							// 	Kind:       "web",
							// 	Payload:    newPayload,
							// }, "", "  "); err == nil {
							// 	fmt.Printf("enriched_web_item:\n%s\n", string(b))
							// }
							if search_id_list != "" {
								if b, err := json.Marshal(importModel.CombinedItem{
									SeqNo:      combined_global[idx].SeqNo,
									IsCitation: combined_global[idx].IsCitation,
									Kind:       "web",
									Payload:    newPayload,
								}); err == nil {
									combined_global[idx].Payload = newPayload
									sendToClient(search_id_list, string(b)+search_id_list+"\n")
								}
							}

						}()
						continue
					}
					// Enrich all items whose type is video, regardless of citation flag
					// if combined_global[i].Kind != "video" {
					// 	continue
					// }

					//this sleep is for the rate limit of the scrapingdog api if it is too low then due to concurrent request scrappingdog will give us 429 error code
					//time.Sleep(1 * time.Second)
					wg.Add(1)
					go func() {
						defer wg.Done()
						if vid, ok := ExtractYouTubeID(urlStr); ok {
							ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
							defer cancel()

							// minimal retry: if 429, wait briefly and try once more
							sd, err := FetchYouTubeDetails(ctx, vid)
							if err != nil && strings.Contains(err.Error(), "status 429") {
								time.Sleep(1 * time.Second)
								fmt.Println("ScrapingDog rate limit hit, retrying...", vid)
								sd, err = FetchYouTubeDetails(ctx, vid)
							}

							if err == nil {
								enr := BuildEnrichment(sd)
								// Mirror image/web assignment: take fallbacks from combined_global[idx]
								current := combined_global[idx].Payload.(importModel.VideoItem)
								newPayload := importModel.VideoItem{
									VideoID:         chooseString(enr.VideoID, current.VideoID),
									URL:             current.URL,
									ThumbnailWidth:  chooseInt(enr.ThumbnailWidth, current.ThumbnailWidth),
									ThumbnailHeight: chooseInt(enr.ThumbnailHeight, current.ThumbnailHeight),
									ThumbnailURL:    chooseString(enr.ThumbnailURL, current.ThumbnailURL),
									Title:           chooseString(enr.Title, current.Title),
									PublishDate:     chooseString(enr.PublishDate, current.PublishDate),
									Likes:           chooseInt(enr.Likes, current.Likes),
									Views:           chooseInt(enr.Views, current.Views),
									Description:     chooseString(enr.Description, current.Description),
								}
								// if b, err := json.MarshalIndent(importModel.CombinedItem{
								// 	SeqNo:      combined_global[idx].SeqNo,
								// 	IsCitation: combined_global[idx].IsCitation,
								// 	Kind:       "video",
								// 	Payload:    newPayload,
								// }, "", "  "); err == nil {
								// 	fmt.Printf("enriched_video_item:\n%s\n", string(b))
								// }
								if search_id_list != "" {
									if b, err := json.Marshal(importModel.CombinedItem{
										SeqNo:      combined_global[idx].SeqNo,
										IsCitation: combined_global[idx].IsCitation,
										Kind:       "video",
										Payload:    newPayload,
									}); err == nil {
										//here also update the combined list
										combined_global[idx].Payload = newPayload
										sendToClient(search_id_list, string(b)+search_id_list+"\n")
										//fmt.Println(b)
									}
								}
							} else {
								fmt.Printf("ScrapingDog error for seq_no=%d url=%s: %v\n", combined_global[idx].SeqNo, urlStr, err)
								// On repeated failure, send a fallback update marking not processed
								if search_id_list != "" {
									if current, ok := combined_global[idx].Payload.(importModel.VideoItem); ok {
										fallback := importModel.VideoItem{
											VideoID:         current.VideoID,
											URL:             current.URL,
											ThumbnailWidth:  current.ThumbnailWidth,
											ThumbnailHeight: current.ThumbnailHeight,
											ThumbnailURL:    current.ThumbnailURL,
											Title:           current.Title,
											PublishDate:     current.PublishDate,
											Likes:           current.Likes,
											Views:           current.Views,
											Description:     current.Description,
											IsNotProcessed:  true,
										}
										if b, err := json.Marshal(importModel.CombinedItem{
											SeqNo:      combined_global[idx].SeqNo,
											IsCitation: combined_global[idx].IsCitation,
											Kind:       "video",
											Payload:    fallback,
										}); err == nil {
											combined_global[idx].Payload = fallback
											sendToClient(search_id_list, string(b)+search_id_list+"\n")
										}
									}
								}

							}
						}
					}()
				}
			}(chunk)

		}
		if logFn != nil {

			if content != "" && search_id_ai_summary != "" {
				//fmt.Println("content:------->", content)
				sendToClient(search_id_ai_summary, content+search_id_ai_summary+"\n")
			}
		}
		if content != "" {
			lastNonEmptyContent = content
			// 	// Append AI content to a per-request text file
			//_ = AppendContentToFile(search_id_ai_summary, content)
		}
		// If the provider signals completion via finish_reason in the last chunk, end the loop
		if len(chunk.Choices) > 0 && chunk.Choices[0].FinishReason != nil {
			if *chunk.Choices[0].FinishReason == "stop" {
				// Optionally emit a final SSE record and finish
				_ = writeSSE(map[string]any{
					"object":        chunk.Object,
					"finish_reason": "stop",
					"ts":            time.Now().UnixMilli(),
				})
				// if lastNonEmptyContent != "" {
				// 	fmt.Printf("🧩 Last content chunk for %s: %q\n", search_id_ai_summary, lastNonEmptyContent)
				// } else {
				// 	fmt.Printf("🧩 No non-empty content captured for %s before stop\n", search_id_ai_summary)
				// }
				//waiting for the lists to be enriched with the json coming from the extractors
				wg.Wait()
				//Print combined list once more at stop using the latest snapshot
				// if b, err := json.MarshalIndent(combined_global, "", "  "); err == nil {
				// 	fmt.Printf("combined_list:\n%s\n", string(b))
				// 	// if search_id_list != "" {
				// 	// 	sendToClient(search_id_list, string(b)+"\n")
				// 	// }
				// }
				//sending the list to the ytdl extractor
				go func() {
					ProcessVideoExtraction(combined_global, searchRecord, platoform)
				}()

				if search_id_ai_summary != "" {
					DisconnectClient(search_id_ai_summary)
				}
				if search_id_list != "" {
					DisconnectClient(search_id_list)
				}
				// Persist response and items if DB is available and search record present
				if pool != nil && searchRecord != nil {
					sr, err := database.CreateSearchResponse(ctx, pool, searchRecord.SearchID, &lastNonEmptyContent)
					if err != nil {
						fmt.Printf("db: create search_response failed: %v\n", err)
					} else {
						for i := range combined_global {
							item := combined_global[i]
							seq := item.SeqNo
							switch item.Kind {
							case "video":
								switch v := item.Payload.(type) {
								case importModel.VideoItem:
									vid, err := database.InsertVideoURL(ctx, pool, sr.ResponseID, item.IsCitation, &seq)
									if err != nil {
										fmt.Printf("db: insert video_urls failed: %v\n", err)
										continue
									}
									url := v.URL
									title := v.Title
									thumb := v.ThumbnailURL
									pub := database.ParseDatePtr(v.PublishDate)
									likes := int64(v.Likes)
									views := int64(v.Views)
									desc := v.Description
									vm := importModel.VideoMetadata{VideoID: vid, VideoURL: &url, Title: &title, Thumbnail: &thumb, PublishDate: pub, Likes: &likes, Views: &views, Description: &desc}
									if err := database.InsertVideoMetadata(ctx, pool, vid, vm); err != nil {
										fmt.Printf("db: insert video_metadata failed: %v\n", err)
									}
								case *importModel.VideoItem:
									if v == nil {
										fmt.Printf("db: skip video nil payload at seq=%d\n", seq)
										continue
									}
									vid, err := database.InsertVideoURL(ctx, pool, sr.ResponseID, item.IsCitation, &seq)
									if err != nil {
										fmt.Printf("db: insert video_urls failed: %v\n", err)
										continue
									}
									url := v.URL
									title := v.Title
									thumb := v.ThumbnailURL
									pub := database.ParseDatePtr(v.PublishDate)
									likes := int64(v.Likes)
									views := int64(v.Views)
									desc := v.Description
									vm := importModel.VideoMetadata{VideoID: vid, VideoURL: &url, Title: &title, Thumbnail: &thumb, PublishDate: pub, Likes: &likes, Views: &views, Description: &desc}
									if err := database.InsertVideoMetadata(ctx, pool, vid, vm); err != nil {
										fmt.Printf("db: insert video_metadata failed: %v\n", err)
									}
								default:
									// unsupported payload shape
								}
							case "web":
								switch w := item.Payload.(type) {
								case importModel.WebItem:
									if err := database.InsertWebURL(ctx, pool, sr.ResponseID, w.URL, item.IsCitation, &seq); err != nil {
										fmt.Printf("db: insert web_urls failed: %v\n", err)
									}
									if err := database.InsertWebMetadata(ctx, pool, w); err != nil {
										fmt.Printf("db: insert web_metadata failed: %v\n", err)
									}
								case *importModel.WebItem:
									if w == nil {
										fmt.Printf("db: skip web nil payload at seq=%d\n", seq)
										continue
									}
									if err := database.InsertWebURL(ctx, pool, sr.ResponseID, w.URL, item.IsCitation, &seq); err != nil {
										fmt.Printf("db: insert web_urls failed: %v\n", err)
									}
									if err := database.InsertWebMetadata(ctx, pool, *w); err != nil {
										fmt.Printf("db: insert web_metadata failed: %v\n", err)
									}
								default:
									// unsupported payload shape
								}
							case "image":
								switch im := item.Payload.(type) {
								case importModel.ImageItem:
									if err := database.InsertImageURL(ctx, pool, sr.ResponseID, im.OriginURL, item.IsCitation, &seq); err != nil {
										fmt.Printf("db: insert image_urls failed: %v\n", err)
									}
									if err := database.InsertImageMetadata(ctx, pool, im); err != nil {
										fmt.Printf("db: insert image_metadata failed: %v\n", err)
									}
								case *importModel.ImageItem:
									if im == nil {
										fmt.Printf("db: skip image nil payload at seq=%d\n", seq)
										continue
									}
									if err := database.InsertImageURL(ctx, pool, sr.ResponseID, im.OriginURL, item.IsCitation, &seq); err != nil {
										fmt.Printf("db: insert image_urls failed: %v\n", err)
									}
									if err := database.InsertImageMetadata(ctx, pool, *im); err != nil {
										fmt.Printf("db: insert image_metadata failed: %v\n", err)
									}
								default:
									// unsupported payload shape
								}
							}
						}
					}
				}

				return nil
			}
		}
		_ = writeSSE(map[string]any{
			"object":  chunk.Object,
			"content": content,
			"ts":      time.Now().UnixMilli(),
		})
	}
}

// MustGetPerplexityKey reads the api key from env and panics if missing (caller responsibility to handle startup env).
func MustGetPerplexityKey() string {
	key := os.Getenv("Perplexity_API_Key")
	return key
}

// BuildMessages constructs messages with a system prompt and alternating user/assistant roles from keywords.
// Ensures the final message role is user to satisfy Perplexity requirements.
func BuildMessages(systemPrompt string, keywords []string) []importModel.ChatMessage {
	msgs := make([]importModel.ChatMessage, 0, len(keywords)+1)
	if systemPrompt != "" {
		msgs = append(msgs, importModel.ChatMessage{Role: "system", Content: systemPrompt})
	}
	role := "user"
	for i := 0; i < len(keywords); i++ {
		msgs = append(msgs, importModel.ChatMessage{Role: role, Content: keywords[i]})
		if role == "user" {
			role = "assistant"
		} else {
			role = "user"
		}
	}
	if len(keywords) > 0 {
		if role == "user" { // last appended was assistant
			msgs = append(msgs, importModel.ChatMessage{Role: "user", Content: "Please continue."})
		}
	}
	return msgs
}

// StartBackgroundPerplexity starts a goroutine that streams from Perplexity and logs each chunk via logFn.
// It uses io.Discard for output (no client streaming) and returns immediately.
func StartBackgroundPerplexity(parent context.Context, apiKey string, req importModel.PerplexityRequest, timeout time.Duration, search_id_ai_summary string, search_id_list string, searchRecord *importModel.Search, pool *pgxpool.Pool, platform string, logFn func(object, content string)) {
	// Wait briefly for TCP client registration to avoid missing early chunks
	//if searchRecord != nil {
	//id := searchRecord.SearchID.String()
	ready := WaitForClientRegistered(search_id_ai_summary, 5*time.Second)
	if !ready {
		fmt.Println("⏱️ TCP client not registered within 2s for searchID:", search_id_ai_summary)
	} else {
		fmt.Println("✅ TCP client registered for searchID:", search_id_ai_summary)
	}
	ready = WaitForClientRegistered(search_id_list, 5*time.Second)
	if !ready {
		fmt.Println("⏱️ TCP client not registered within 2s for searchID:", search_id_list)
	} else {
		fmt.Println("✅ TCP client registered for searchID:", search_id_list)
	}
	//time.Sleep(20 * time.Second)
	//}
	go func() {
		ctx := parent
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(parent, timeout)
			defer cancel()
		}
		body, _, err := StreamPerplexity(ctx, nil, apiKey, req)
		if err != nil {
			fmt.Printf("Perplexity error: %v\n", err)
			// Immediately close any registered TCP clients for this search to avoid dangling sockets
			if search_id_ai_summary != "" {
				DisconnectClient(search_id_ai_summary)
			}
			if search_id_list != "" {
				DisconnectClient(search_id_list)
			}
			return
		}
		defer body.Close()
		// fmt.Println("--------------------------------")
		// fmt.Println("--------------------------------")
		// fmt.Println("--------------------------------")
		// fmt.Println("--------------------------------")
		// fmt.Println("--------------------------------")
		// fmt.Println("--------------------------------")
		// fmt.Println("SearchID that has sent to the client:", search_id_ai_summary)
		// fmt.Println("--------------------------------")
		// fmt.Println("--------------------------------")
		// fmt.Println("--------------------------------")
		// fmt.Println("--------------------------------")
		// fmt.Println("--------------------------------")
		// fmt.Println("--------------------------------")

		_ = ForwardStream(ctx, body, io.Discard, logFn, search_id_ai_summary, search_id_list, pool, searchRecord, platform)
	}()
}

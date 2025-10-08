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

	importModel "github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/models"
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

// wrapped update structure to preserve JSON field order
type videoUpdate struct {
	SeqNo      int                   `json:"seq_no"`
	IsCitation bool                  `json:"is_citation"`
	Kind       string                `json:"kind"`
	Payload    importModel.VideoItem `json:"payload"`
}

const perplexityURL = "https://api.perplexity.ai/chat/completions"

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
	select {
	case client.MsgCh <- chunk:
		// Debug print suppressed
	default:
		// Debug print suppressed
	}
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
	// Close channel safely in case it's already closed
	func() {
		defer func() { _ = recover() }()
		close(client.MsgCh)
	}()
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
func ForwardStream(ctx context.Context, r io.Reader, w io.Writer, logFn func(object string, content string), search_id_ai_summary, search_id_list string) error {
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
				combined := make([]importModel.CombinedItem, 0, 30)
				// search_results classified as video (YouTube) or web; both are citations
				for _, sr := range c.SearchResults {
					isYouTube := strings.Contains(sr.URL, "youtube.com") || strings.Contains(sr.URL, "youtu.be")
					if isYouTube {
						combined = append(combined, importModel.CombinedItem{
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
					combined = append(combined, importModel.CombinedItem{
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
					combined = append(combined, importModel.CombinedItem{
						SeqNo:      seq,
						IsCitation: false,
						Kind:       "image",
						Payload: importModel.ImageItem{
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
					combined = append(combined, importModel.CombinedItem{
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
					combined = append(combined, importModel.CombinedItem{
						SeqNo:      seq,
						IsCitation: false,
						Kind:       "related",
						Payload: importModel.RelatedList{
							Queries: c.RelatedQuestions,
						},
					})
					seq++
				}

				// Print combined list once (pretty printed for readability)
				if b, err := json.MarshalIndent(combined, "", "  "); err == nil {
					fmt.Printf("combined_list:\n%s\n", string(b))
					if search_id_list != "" {
						sendToClient(search_id_list, string(b)+"\n")
					}
				}
				// first socket can be initiated here to send data to extractors (non-blocking)
				// Enrich YouTube video items asynchronously via ScrapingDog

				for i := range combined {
					// Enrich all items whose type is video, regardless of citation flag
					if combined[i].Kind != "video" {
						continue
					}
					v, ok := combined[i].Payload.(importModel.VideoItem)
					if !ok {
						continue
					}
					if v.URL == "" || !(strings.Contains(v.URL, "youtube.com") || strings.Contains(v.URL, "youtu.be")) {
						continue
					}
					idx := i
					urlStr := v.URL
					time.Sleep(1 * time.Second)
					go func() {
						if vid, ok := ExtractYouTubeID(urlStr); ok {
							ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
							defer cancel()
							if sd, err := FetchYouTubeDetails(ctx, vid); err == nil {
								enr := BuildEnrichment(sd)
								newPayload := importModel.VideoItem{
									VideoID:         chooseString(enr.VideoID, v.VideoID),
									URL:             v.URL,
									ThumbnailWidth:  chooseInt(enr.ThumbnailWidth, v.ThumbnailWidth),
									ThumbnailHeight: chooseInt(enr.ThumbnailHeight, v.ThumbnailHeight),
									ThumbnailURL:    chooseString(enr.ThumbnailURL, v.ThumbnailURL),
									Title:           chooseString(enr.Title, v.Title),
									Thumbnail:       v.Thumbnail,
									PublishDate:     chooseString(enr.PublishDate, v.PublishDate),
									Likes:           chooseInt(enr.Likes, v.Likes),
									Views:           chooseInt(enr.Views, v.Views),
									Description:     chooseString(enr.Description, v.Description),
								}
								if b, err := json.MarshalIndent(videoUpdate{
									SeqNo:      combined[idx].SeqNo,
									IsCitation: combined[idx].IsCitation,
									Kind:       "video",
									Payload:    newPayload,
								}, "", "  "); err == nil {
									fmt.Printf("enriched_video_item:\n%s\n", string(b))
								}
								if search_id_list != "" {
									if b, err := json.Marshal(videoUpdate{
										SeqNo:      combined[idx].SeqNo,
										IsCitation: combined[idx].IsCitation,
										Kind:       "video",
										Payload:    newPayload,
									}); err == nil {
										sendToClient(search_id_list, string(b)+"\n")
									}
								}
							} else {
								fmt.Printf("ScrapingDog error for seq_no=%d url=%s: %v\n", combined[idx].SeqNo, urlStr, err)
							}
						}
					}()
				}
			}(chunk)

		}
		if logFn != nil {
			// logFn(chunk.Object, content) // content printing disabled per request
			// second socket can stream chunk content if needed
			// server go routine to send data to the client (TCP broadcast)
			// if content != "" {
			// 	broadcastTCPLine(content + "\n")
			// }
			if content != "" && search_id_ai_summary != "" {
				sendToClient(search_id_ai_summary, content+"\n")
			}
		}
		if content != "" {
			lastNonEmptyContent = content
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
				if lastNonEmptyContent != "" {
					fmt.Printf("🧩 Last content chunk for %s: %q\n", search_id_ai_summary, lastNonEmptyContent)
				} else {
					fmt.Printf("🧩 No non-empty content captured for %s before stop\n", search_id_ai_summary)
				}
				if search_id_ai_summary != "" {
					DisconnectClient(search_id_ai_summary)
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
func StartBackgroundPerplexity(parent context.Context, apiKey string, req importModel.PerplexityRequest, timeout time.Duration, search_id_ai_summary string, search_id_list string, searchRecord *importModel.Search, logFn func(object, content string)) {
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
			return
		}
		defer body.Close()
		fmt.Println("--------------------------------")
		fmt.Println("--------------------------------")
		fmt.Println("--------------------------------")
		fmt.Println("--------------------------------")
		fmt.Println("--------------------------------")
		fmt.Println("--------------------------------")
		fmt.Println("SearchID that has sent to the client:", search_id_ai_summary)
		fmt.Println("--------------------------------")
		fmt.Println("--------------------------------")
		fmt.Println("--------------------------------")
		fmt.Println("--------------------------------")
		fmt.Println("--------------------------------")
		fmt.Println("--------------------------------")

		_ = ForwardStream(ctx, body, io.Discard, logFn, search_id_ai_summary, search_id_list)
	}()
}

package cli

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client is a thin HTTP wrapper around the Nagare API.
// It is the single point of contact between the CLI and the server —
// no direct imports of models/store/scheduler.
type Client struct {
	BaseURL string
	APIKey  string
	HTTP    *http.Client
}

// NewClient creates a Client with sensible defaults.
func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		HTTP:    &http.Client{Timeout: 30 * time.Second},
	}
}

// Get performs an authenticated GET and returns the raw response body.
func (c *Client) Get(path string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	return c.do(req)
}

// Post performs an authenticated POST with an optional JSON body.
func (c *Client) Post(path string, body interface{}) ([]byte, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshaling request body: %w", err)
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(http.MethodPost, c.BaseURL+path, r)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.do(req)
}

// StreamSSE connects to an SSE endpoint and calls handler for each data line.
// It blocks until the stream ends or an error occurs.
func (c *Client) StreamSSE(path string, handler func(line string)) error {
	req, err := http.NewRequest(http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return err
	}
	c.setAuth(req)
	req.Header.Set("Accept", "text/event-stream")

	// Use a client without timeout for long-lived streams.
	streamClient := &http.Client{}
	resp, err := streamClient.Do(req)
	if err != nil {
		return fmt.Errorf("connecting to log stream: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			handler(strings.TrimPrefix(line, "data: "))
		}
	}
	return scanner.Err()
}

func (c *Client) do(req *http.Request) ([]byte, error) {
	c.setAuth(req)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("server returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func (c *Client) setAuth(req *http.Request) {
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
}

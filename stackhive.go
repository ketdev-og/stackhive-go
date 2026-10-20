package stackhive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Config holds the settings for a Client. BaseURL and APIKey are required.
type Config struct {
	// BaseURL is the gateway, for example "https://api.example.com".
	BaseURL string

	// APIKey is a key of the form "ak_live_…". Read it from configuration or
	// the environment, never from a literal in source.
	APIKey string

	// HTTPClient is optional — supply one for a proxy, a custom TLS setup or
	// shared transport. It must not set an overall Timeout: an upload is one
	// long request, and a client-wide timeout would cut large files off. Bound
	// transfers with the context instead.
	HTTPClient *http.Client

	// MaxAttempts is how many attempts that achieve nothing are allowed before
	// giving up. Attempts that move the transfer forward do not count, so this
	// limits being stuck rather than total retries. Default 8.
	MaxAttempts int

	// RequestTimeout bounds the small JSON calls. Transfers are bounded by the
	// caller's context instead. Default 30 seconds.
	RequestTimeout time.Duration
}

// Client talks to the service. It is safe for concurrent use; build one and
// share it rather than creating one per call, since it holds a connection pool.
type Client struct {
	baseURL        string
	apiKey         string
	http           *http.Client
	maxAttempts    int
	requestTimeout time.Duration

	// key is what the service said about the API key, fetched once: a key's
	// workspace never changes.
	keyMu sync.Mutex
	key   *KeyInfo
}

// New validates the configuration and builds a client.
//
// It refuses a client that cannot work — no address, no key — so the mistake
// surfaces at startup rather than on the first upload.
func New(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, errors.New("stackhive: BaseURL is required")
	}
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, errors.New("stackhive: APIKey is required")
	}

	c := &Client{
		// Trailing slashes are the commonest configuration slip and would
		// produce "//api/storage/…" on every call.
		baseURL:        strings.TrimRight(cfg.BaseURL, "/"),
		apiKey:         cfg.APIKey,
		http:           cfg.HTTPClient,
		maxAttempts:    cfg.MaxAttempts,
		requestTimeout: cfg.RequestTimeout,
	}
	if c.http == nil {
		// No Timeout on purpose — see Config.HTTPClient.
		c.http = &http.Client{}
	}
	if c.maxAttempts <= 0 {
		c.maxAttempts = 8
	}
	if c.requestTimeout <= 0 {
		c.requestTimeout = 30 * time.Second
	}
	return c, nil
}

// ------------------------------------------------------------- objects

// List returns the objects this API key's owner has stored.
//
// The service wraps the list as {"objects": [...]}, so the array is read out of
// that field rather than expected at the top level.
func (c *Client) List(ctx context.Context) ([]StoredObject, error) {
	var response struct {
		Objects []StoredObject `json:"objects"`
	}
	if err := c.call(ctx, http.MethodGet, "/api/storage/objects", nil, &response); err != nil {
		return nil, err
	}
	return response.Objects, nil
}

// Get fetches one object's details.
//
// A missing object and one belonging to someone else both come back as
// ErrNotFound — the service does not distinguish the two.
func (c *Client) Get(ctx context.Context, objectID string) (*StoredObject, error) {
	var obj StoredObject
	if err := c.call(ctx, http.MethodGet, "/api/storage/objects/"+url.PathEscape(objectID), nil, &obj); err != nil {
		return nil, err
	}
	return &obj, nil
}

// ------------------------------------------------------- shared plumbing

// busyRetries is how many times a call answered 429 (rate limited) or 503
// (busy) is tried again. Neither answer means the request was carried out,
// so trying again is safe.
const busyRetries = 4

// call sends one control-plane request and decodes the JSON answer.
//
// One place for the Authorization header, the timeout, retrying a busy
// service and the mapping of failures to APIError, so every call behaves the
// same way. Only small JSON passes through here; file content never does.
func (c *Client) call(ctx context.Context, method, path string, in, out any) error {
	var encoded []byte
	if in != nil {
		var err error
		if encoded, err = json.Marshal(in); err != nil {
			return fmt.Errorf("stackhive: encoding request: %w", err)
		}
	}

	for attempt := 0; ; attempt++ {
		status, header, data, err := c.send(ctx, method, path, encoded)
		if err != nil {
			return err
		}
		if (status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable) && attempt < busyRetries {
			if err := wait(ctx, retryDelay(header.Get("Retry-After"), attempt)); err != nil {
				return err
			}
			continue
		}
		if status < 200 || status >= 300 {
			return newAPIError(status, data)
		}
		if out != nil && len(data) > 0 {
			if err := json.Unmarshal(data, out); err != nil {
				return fmt.Errorf("stackhive: decoding response: %w", err)
			}
		}
		return nil
	}
}

// send makes one attempt at a call, bounded by RequestTimeout.
func (c *Client) send(ctx context.Context, method, path string, encoded []byte) (int, http.Header, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()

	var body io.Reader
	if encoded != nil {
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("stackhive: building request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")
	if encoded != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("stackhive: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	// Capped, so a misbehaving proxy returning something huge cannot exhaust
	// memory. Every legitimate response here is a few kilobytes.
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return 0, nil, nil, fmt.Errorf("stackhive: reading response: %w", err)
	}
	return resp.StatusCode, resp.Header, data, nil
}

// retryDelay is how long to wait before retrying a busy answer: what its
// Retry-After header asks for (seconds or a date), capped at a minute, or
// the usual backoff when it has none.
func retryDelay(retryAfter string, attempt int) time.Duration {
	retryAfter = strings.TrimSpace(retryAfter)
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		return min(time.Duration(seconds)*time.Second, time.Minute)
	}
	if at, err := http.ParseTime(retryAfter); err == nil {
		return min(max(time.Until(at), 0), time.Minute)
	}
	return backoff(attempt)
}

// abort tells the service to abandon an upload and release its parts.
//
// Best effort, on a fresh context: this runs when the caller has already
// walked away — often because their own context was cancelled — and turning a
// cancel into a second error would not help them. The worst case is parts
// sitting in storage until the token expires.
func (c *Client) abort(objectID string) {
	_ = c.call(context.Background(), http.MethodDelete,
		"/api/storage/uploads/"+url.PathEscape(objectID), nil, nil)
}

// backoff is how long to wait before the next attempt: 1s, 2s, 4s … capped at 30s.
//
// Backing off matters most when the worker is at capacity — retrying hard would
// keep it there.
func backoff(attempt int) time.Duration {
	if attempt > 5 {
		attempt = 5
	}
	d := time.Second << attempt
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

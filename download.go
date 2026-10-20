package stackhive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// DownloadTo fetches an object into a file, resuming from whatever is already
// on disk.
//
// Resume here is a Range request, so it is byte-exact: an interrupted download
// costs nothing, unlike an interrupted upload, which loses up to one part. An
// existing file at target is treated as a partial download and appended to —
// pass a fresh path to start over.
//
// The signed URL expires, so a long transfer can outlive it. A 401 or 403 from
// storage is therefore not a failure: it means fetch a new URL and carry on.
func (c *Client) DownloadTo(ctx context.Context, objectID, target string, opts ...Option) error {
	return c.download(ctx, target, opts, func(ctx context.Context) (string, error) {
		return c.signedURL(ctx, objectID)
	})
}

// download fetches into target from the URL sign returns, asking for a new
// one when it expires mid-transfer.
func (c *Client) download(ctx context.Context, target string, opts []Option, sign func(context.Context) (string, error)) error {
	o := collect(opts)

	signed, err := sign(ctx)
	if err != nil {
		return err
	}

	written := fileSize(target)
	total := int64(-1)
	var lastFailure error

	for attempt := 0; attempt < c.maxAttempts; {
		before := written

		status, err := c.fetchInto(ctx, signed, target, &written, &total, o.progress)

		if err == nil {
			switch status {
			case http.StatusOK, http.StatusPartialContent, http.StatusRequestedRangeNotSatisfiable:
				return nil
			case http.StatusUnauthorized, http.StatusForbidden:
				// The signature aged out mid-download. Not a failure — get a
				// fresh URL. Counted, so a URL that never works still gives up.
				if signed, err = sign(ctx); err != nil {
					return err
				}
				attempt++
				continue
			case http.StatusNotFound:
				return ErrNotFound
			default:
				return &APIError{Status: status, Message: "download failed"}
			}
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
		lastFailure = err

		// Bytes landed, so this was progress rather than a failure and the
		// attempt budget is handed back.
		written = fileSize(target)
		if written > before {
			attempt = 0
		} else {
			attempt++
		}

		if err := wait(ctx, backoff(attempt)); err != nil {
			return err
		}
	}

	return &TransferFailedError{
		Attempts:         c.maxAttempts,
		BytesTransferred: written,
		Err:              lastFailure,
	}
}

// fetchInto makes one attempt at the download and writes what arrives.
//
// It returns the status with a nil error when the server answered — the caller
// decides what each status means — and an error only when the transfer itself
// broke, which is the case worth retrying.
//
// written and total are pointers because they outlive one attempt: written is
// where the next Range starts, total is learned once and kept for progress.
func (c *Client) fetchInto(ctx context.Context, signed, target string,
	written, total *int64, progress ProgressFunc) (int, error) {

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, signed, nil)
	if err != nil {
		return 0, err
	}
	if *written > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", *written))
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
	case http.StatusRequestedRangeNotSatisfiable:
		// Nothing left to send: the file on disk is already whole.
		return resp.StatusCode, nil
	default:
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return resp.StatusCode, nil
	}

	// 200 means the server ignored the Range and is sending the whole file,
	// so anything already on disk is overwritten rather than appended to.
	appending := resp.StatusCode == http.StatusPartialContent && *written > 0
	flags := os.O_CREATE | os.O_WRONLY
	if appending {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
		*written = 0
	}

	if *total < 0 {
		*total = totalLength(resp, *written)
	}

	file, err := os.OpenFile(target, flags, 0o644)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	buf := make([]byte, 64<<10)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, err := file.Write(buf[:n]); err != nil {
				return 0, err
			}
			*written += int64(n)
			progress(*written, *total)
		}
		if readErr == io.EOF {
			return resp.StatusCode, nil
		}
		if readErr != nil {
			return 0, readErr
		}
	}
}

// signedURL asks the API for a short-lived URL pointing at the bytes.
//
// Two steps rather than one because the API decides who may read the object
// and storage serves it — the file never passes through the API.
func (c *Client) signedURL(ctx context.Context, objectID string) (string, error) {
	var response struct {
		URL string `json:"url"`
	}
	path := "/api/storage/objects/" + url.PathEscape(objectID) + "/download-url"
	if err := c.call(ctx, http.MethodGet, path, nil, &response); err != nil {
		return "", err
	}
	if response.URL == "" {
		return "", errors.New("stackhive: the service returned no download URL")
	}
	return response.URL, nil
}

// totalLength works out the file's full size so progress can be a percentage.
//
// On a resumed download Content-Length is only what is left, so Content-Range
// is preferred: it states the total after the slash. Returns -1 when neither
// is usable, which ProgressFunc documents as unknown.
func totalLength(resp *http.Response, written int64) int64 {
	if contentRange := resp.Header.Get("Content-Range"); contentRange != "" {
		if i := strings.LastIndexByte(contentRange, '/'); i >= 0 {
			if n, err := strconv.ParseInt(strings.TrimSpace(contentRange[i+1:]), 10, 64); err == nil {
				return n
			}
		}
	}
	if resp.ContentLength >= 0 {
		return resp.ContentLength + written
	}
	return -1
}

// fileSize is the size of a partial download, or 0 when there is nothing to
// resume from.
func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// wait sleeps for d, or returns early with the context's error if it ends.
func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

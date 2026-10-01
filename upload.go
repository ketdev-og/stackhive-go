package stackhive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// State is where an upload has got to.
type State int

const (
	// Ready means not running. Start begins the upload — or resumes it, if a
	// previous Start was stopped by its context or ran out of attempts.
	Ready State = iota
	// Running means bytes are moving, or the SDK is waiting before another attempt.
	Running
	// Paused means Pause was called. The server-side upload is still open.
	Paused
	// Completed means stored and assembled. Terminal.
	Completed
	// Failed means rejected in a way retrying cannot fix. Terminal.
	Failed
	// Cancelled means Cancel was called and the upload was abandoned. Terminal.
	Cancelled
)

// String gives the state a readable name for logs.
func (s State) String() string {
	switch s {
	case Ready:
		return "ready"
	case Running:
		return "running"
	case Paused:
		return "paused"
	case Completed:
		return "completed"
	case Failed:
		return "failed"
	case Cancelled:
		return "cancelled"
	}
	return "unknown"
}

// Terminal reports whether the state can no longer change.
//
// Used to stop a finished handle being started again, which would open a
// second server-side upload and orphan the first.
func (s State) Terminal() bool {
	return s == Completed || s == Failed || s == Cancelled
}

// errNotStarted means an attempt was skipped because a pause or cancel landed
// just before it began. The upload loop recognises why and handles it.
var errNotStarted = errors.New("stackhive: attempt not started")

// Upload is one upload, and everything needed to see it through.
//
// This is where the resume logic lives. The file is streamed to the service in
// one request; if the connection breaks, the SDK asks the service how far it
// actually got and sends only the rest. The caller never computes an offset,
// never seeks and never sees a part.
//
// Start blocks. Pause, Resume and Cancel are meant to be called from another
// goroutine while it runs.
type Upload struct {
	client      *Client
	path        string
	size        int64
	filename    string
	contentType string
	progress    ProgressFunc

	// offset is what the SERVICE says it holds. It is only ever set from an
	// answer the service gave. Bytes put on the wire is a different number —
	// the worker stores whole parts and discards a part cut short — and
	// resuming from our own count would skip data that was never stored.
	offset atomic.Int64

	// transferred is bytes handed to the connection. It drives the progress
	// bar and nothing else.
	transferred atomic.Int64

	mu           sync.Mutex
	state        State
	streamURL    string
	objectID     string
	paused       chan struct{} // non-nil while paused; closed by Resume
	pauseSignal  chan struct{} // wakes a backoff sleep when Pause is called
	cancelled    chan struct{} // closed by Cancel
	cancelOnce   sync.Once
	abortAttempt context.CancelFunc // ends the request in flight
}

// NewUpload prepares an upload without starting it.
//
// Take this when you want pause, resume, cancel or progress, or want to run the
// transfer on its own goroutine. Otherwise UploadFile is the whole job in one
// call. The file is only read for its size here; nothing is sent.
func (c *Client) NewUpload(path string, opts ...Option) (*Upload, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stackhive: %w", err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("stackhive: %s is a directory", path)
	}

	o := collect(opts)
	return &Upload{
		client:      c,
		path:        path,
		size:        info.Size(),
		filename:    filepath.Base(path),
		contentType: contentTypeOf(path),
		progress:    o.progress,
		pauseSignal: make(chan struct{}, 1),
		cancelled:   make(chan struct{}),
	}, nil
}

// UploadFile uploads a file and waits for it.
//
// Resumes by itself on a dropped connection, a timeout or a busy worker; the
// caller is not told and does not need to be. Returns when the file is stored,
// or with an error when it could not be.
func (c *Client) UploadFile(ctx context.Context, path string, opts ...Option) (*StoredObject, error) {
	up, err := c.NewUpload(path, opts...)
	if err != nil {
		return nil, err
	}
	return up.Start(ctx)
}

// State reports where the upload has got to.
func (u *Upload) State() State {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.state
}

// Offset reports the bytes the service has confirmed it holds.
//
// Deliberately not "bytes sent": this is the number a resume starts from, and
// it only moves when the service says so.
func (u *Upload) Offset() int64 {
	return u.offset.Load()
}

// Size is the file's size in bytes.
func (u *Upload) Size() int64 {
	return u.size
}

// ObjectID is the object's id once the upload has been opened, or "" before.
func (u *Upload) ObjectID() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.objectID
}

// Start sends the file, resuming as many times as it takes.
//
// The loop's one rule: the offset always comes from the service. From the
// Upload-Offset header when a response arrived, from HEAD when the connection
// died before one could. Guessing would risk skipping bytes never stored.
//
// Attempts are only spent on attempts that achieve nothing. A transfer that
// keeps inching forward over a bad line finishes; one that is truly stuck
// gives up with a TransferFailedError.
//
// Cancelling ctx stops the upload without abandoning it: the handle goes back
// to Ready, and calling Start again picks up where it stopped. Use Cancel to
// abandon it for good.
func (u *Upload) Start(ctx context.Context) (*StoredObject, error) {
	u.mu.Lock()
	switch {
	case u.state.Terminal():
		state := u.state
		u.mu.Unlock()
		return nil, fmt.Errorf("stackhive: this upload is %s and cannot be restarted", state)
	case u.state != Ready:
		u.mu.Unlock()
		return nil, errors.New("stackhive: this upload is already running")
	}
	u.state = Running
	u.mu.Unlock()

	if err := u.ensurePlan(ctx); err != nil {
		return nil, u.finish(err)
	}

	var lastFailure error
	attempt := 0

	for attempt < u.client.maxAttempts {
		if err := u.waitIfPaused(ctx); err != nil {
			return nil, u.finish(err)
		}
		if u.isCancelled() {
			return nil, u.finish(ErrCancelled)
		}
		before := u.offset.Load()

		result, err := u.putFrom(ctx, before)

		switch {
		case err != nil:
			// A Cancel or a Pause also ends the request, so rule those out
			// before treating this as a network fault.
			if u.isCancelled() {
				return nil, u.finish(ErrCancelled)
			}
			if u.isPauseRequested() {
				continue // the top of the loop parks; a pause is not a failed attempt
			}
			if ctx.Err() != nil {
				return nil, u.finish(ctx.Err())
			}
			lastFailure = err
			stored, herr := u.headOffset(ctx) // no response came back; go and ask
			if herr != nil {
				return nil, u.finish(herr)
			}
			u.offset.Store(stored)

		case result.status == http.StatusCreated:
			var obj StoredObject
			if err := json.Unmarshal(result.body, &obj); err != nil {
				return nil, u.finish(fmt.Errorf("stackhive: decoding the completed upload: %w", err))
			}
			u.offset.Store(u.size)
			u.progress(u.size, u.size)
			u.setState(Completed)
			return &obj, nil

		case result.status == http.StatusUnauthorized || result.status == http.StatusForbidden:
			return nil, u.finish(newAPIError(result.status, result.body))

		case result.status == http.StatusNotFound:
			return nil, u.finish(ErrExpired)

		default:
			// Everything else is recoverable: 400 interrupted, 499 ended early,
			// 409 wrong offset, 503 worker busy. A 409 even carries the correct
			// offset, so reading it here fixes that case without special care.
			lastFailure = newAPIError(result.status, result.body)
			stored, herr := u.serverOffset(ctx, result)
			if herr != nil {
				return nil, u.finish(herr)
			}
			u.offset.Store(stored)
		}

		if u.offset.Load() > before {
			attempt = 0
		} else {
			attempt++
		}

		if err := u.sleep(ctx, backoff(attempt)); err != nil {
			return nil, u.finish(err)
		}
	}

	return nil, u.finish(&TransferFailedError{
		Attempts:         u.client.maxAttempts,
		BytesTransferred: u.offset.Load(),
		Err:              lastFailure,
	})
}

// ------------------------------------------------------------- control

// Pause stops sending.
//
// There is no pause on the wire, so this works by ending the request in
// flight. Parts already stored stay where they are, and the server-side
// upload remains open until its token expires.
//
// It is not free: the part being filled when the connection closes is
// discarded, so resuming re-sends up to one part.
func (u *Upload) Pause() {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.state != Running || u.paused != nil {
		return
	}
	u.paused = make(chan struct{})
	if u.abortAttempt != nil {
		u.abortAttempt()
	}
	select {
	case u.pauseSignal <- struct{}{}:
	default:
	}
}

// Resume carries on from where the service says the upload stands.
//
// Does nothing when the upload is not paused, so calling it twice is safe.
func (u *Upload) Resume() {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.paused == nil {
		return
	}
	close(u.paused)
	u.paused = nil
}

// Cancel abandons the upload and tells the service to release the parts.
//
// Without the release the parts would sit in storage until the token expired,
// costing space nobody will use. Cancelling a completed upload does nothing.
func (u *Upload) Cancel() {
	// Closed before taking the lock, so an attempt about to start either sees
	// it under the lock or has already registered abortAttempt for us to call.
	u.cancelOnce.Do(func() { close(u.cancelled) })

	u.mu.Lock()
	if u.state == Completed {
		u.mu.Unlock()
		return
	}
	abort := u.abortAttempt
	objectID := u.objectID
	u.state = Cancelled
	u.mu.Unlock()

	if abort != nil {
		abort()
	}
	if objectID != "" {
		u.client.abort(objectID)
	}
}

// waitIfPaused parks the upload until Resume, Cancel or the context ends.
//
// It refreshes the offset first: the connection was cut mid-part, and what the
// service kept is the only number worth resuming from.
func (u *Upload) waitIfPaused(ctx context.Context) error {
	u.mu.Lock()
	ch := u.paused
	if ch != nil {
		u.state = Paused
	}
	u.mu.Unlock()

	if ch == nil {
		return nil
	}

	stored, err := u.headOffset(ctx)
	if err != nil {
		return err
	}
	u.offset.Store(stored)

	select {
	case <-ch:
	case <-u.cancelled:
		return ErrCancelled
	case <-ctx.Done():
		return ctx.Err()
	}

	// Drop a stale pause signal so it cannot cut a later backoff short.
	select {
	case <-u.pauseSignal:
	default:
	}
	u.setState(Running)
	return nil
}

// sleep waits out a backoff, waking early for a pause, a cancel or the context.
//
// A pause returns nil so the loop's next iteration parks properly; the others
// return the reason to stop.
func (u *Upload) sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-u.pauseSignal:
		return nil
	case <-u.cancelled:
		return ErrCancelled
	case <-ctx.Done():
		return ctx.Err()
	}
}

// finish records how the upload ended and passes the error back.
//
// The distinction matters for what the caller can do next. A stopped context or
// running out of attempts leaves the server-side upload intact, so the handle
// returns to Ready and Start resumes it. A cancel is final. Anything else —
// a rejected key, an expired token — cannot be fixed by retrying.
func (u *Upload) finish(err error) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	var transfer *TransferFailedError
	switch {
	case errors.Is(err, ErrCancelled):
		u.state = Cancelled
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded),
		errors.As(err, &transfer):
		u.state = Ready
		u.paused = nil
	default:
		u.state = Failed
	}
	return err
}

// setState changes the state under the lock, because State() may be read from
// another goroutine at the same moment.
func (u *Upload) setState(s State) {
	u.mu.Lock()
	u.state = s
	u.mu.Unlock()
}

// isCancelled reports whether Cancel has been called, without blocking.
//
// A closed channel rather than a flag, so the same signal can also wake a
// goroutine that is waiting in a select.
func (u *Upload) isCancelled() bool {
	select {
	case <-u.cancelled:
		return true
	default:
		return false
	}
}

// isPauseRequested reports whether Pause has been called and not yet resumed.
//
// Used after a failed attempt to tell a deliberate pause — which also ends the
// request — apart from a real network fault.
func (u *Upload) isPauseRequested() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.paused != nil
}

// ---------------------------------------------------------------- wire

// ensurePlan opens the upload, once.
//
// "stream": true is what asks for a single resumable URL rather than a list of
// part URLs. Called again on a resumed Start it does nothing, so the same
// server-side upload is reused and the parts already stored still count.
func (u *Upload) ensurePlan(ctx context.Context) error {
	u.mu.Lock()
	planned := u.streamURL != ""
	u.mu.Unlock()
	if planned {
		return nil
	}

	var plan struct {
		ObjectID  string `json:"objectId"`
		StreamURL string `json:"streamUrl"`
	}
	err := u.client.call(ctx, http.MethodPost, "/api/storage/uploads", map[string]any{
		"filename": u.filename,
		"mime":     u.contentType,
		"size":     u.size,
		"stream":   true,
	}, &plan)
	if err != nil {
		return err
	}
	if plan.StreamURL == "" {
		return errors.New(`stackhive: the service returned no streamUrl; does it support "stream": true?`)
	}

	u.mu.Lock()
	u.objectID = plan.ObjectID
	u.streamURL = plan.StreamURL
	u.mu.Unlock()
	u.offset.Store(0)
	return nil
}

// putResult is what came back from one attempt to send the file.
type putResult struct {
	status int
	body   []byte
	offset string // the Upload-Offset header, if the service sent one
}

// putFrom sends the file from byte `from` to the end, in one request.
//
// No Authorization header: the token inside the stream URL is the credential,
// and this request does not go through the gateway.
//
// The file is opened and seeked here — that Seek is the whole of "stream the
// difference in bytes". The body is a reader over the file with its length
// stated, so the client streams it rather than loading the file into memory: a
// 2 GB upload costs a few kilobytes.
//
// Each attempt gets its own context so Pause and Cancel can end just this
// request without touching the caller's context.
func (u *Upload) putFrom(ctx context.Context, from int64) (*putResult, error) {
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Registered under the lock, and refused if a pause or cancel already
	// landed — otherwise one arriving between the loop's check and here would
	// find nothing to abort and the request would run on regardless.
	u.mu.Lock()
	if u.paused != nil || u.isCancelled() {
		u.mu.Unlock()
		return nil, errNotStarted
	}
	u.abortAttempt = cancel
	streamURL := u.streamURL
	u.mu.Unlock()

	defer func() {
		u.mu.Lock()
		u.abortAttempt = nil
		u.mu.Unlock()
	}()

	file, err := os.Open(u.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if _, err := file.Seek(from, io.SeekStart); err != nil {
		return nil, err
	}

	remaining := u.size - from
	var body io.Reader = &countingReader{reader: file, upload: u, sent: from}
	if remaining <= 0 {
		body = http.NoBody
	}

	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPut, streamURL, body)
	if err != nil {
		return nil, err
	}
	req.ContentLength = remaining
	req.Header.Set("Upload-Offset", strconv.FormatInt(from, 10))
	req.Header.Set("Content-Type", "application/octet-stream")

	u.transferred.Store(from)

	resp, err := u.client.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return &putResult{
		status: resp.StatusCode,
		body:   data,
		offset: resp.Header.Get("Upload-Offset"),
	}, nil
}

// serverOffset reads how far the service got, preferring the answer it gave.
//
// Every response carries Upload-Offset, so the usual resume needs no extra
// request. HEAD is the fallback for a response that arrived without it.
func (u *Upload) serverOffset(ctx context.Context, r *putResult) (int64, error) {
	if r.offset != "" {
		if n, err := strconv.ParseInt(strings.TrimSpace(r.offset), 10, 64); err == nil {
			return n, nil
		}
	}
	return u.headOffset(ctx)
}

// headOffset asks the service how far the upload stands.
//
// The answer is counted from the parts storage actually holds, so it is right
// even after the worker restarted or a different instance answers.
//
// Only an expired token is an error. Any other failure keeps the offset we
// had: the next attempt either succeeds or asks again.
func (u *Upload) headOffset(ctx context.Context) (int64, error) {
	current := u.offset.Load()

	u.mu.Lock()
	streamURL := u.streamURL
	u.mu.Unlock()

	reqCtx, cancel := context.WithTimeout(ctx, u.client.requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodHead, streamURL, nil)
	if err != nil {
		return current, nil
	}
	resp, err := u.client.http.Do(req)
	if err != nil {
		return current, nil
	}
	resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return current, ErrExpired
	}
	if n, err := strconv.ParseInt(strings.TrimSpace(resp.Header.Get("Upload-Offset")), 10, 64); err == nil {
		return n, nil
	}
	return current, nil
}

// countingReader counts bytes as the HTTP client pulls them from the file, to
// drive the progress bar.
//
// It starts at the resume offset rather than zero, so a resumed upload's bar
// carries on from where it was.
type countingReader struct {
	reader io.Reader
	upload *Upload
	sent   int64
}

// Read passes the read through and reports the new total.
func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.sent += int64(n)
		r.upload.transferred.Store(r.sent)
		r.upload.progress(r.sent, r.upload.size)
	}
	return n, err
}

// contentTypeOf guesses the content type from the file's extension.
//
// Falls back to application/octet-stream, which is what the type means when
// nothing better is known — a wrong guess is not worth failing an upload over.
// Parameters such as "; charset=utf-8" are dropped: the service stores a type,
// not an encoding.
func contentTypeOf(path string) string {
	t := mime.TypeByExtension(filepath.Ext(path))
	if i := strings.IndexByte(t, ';'); i >= 0 {
		t = strings.TrimSpace(t[:i])
	}
	if t == "" {
		return "application/octet-stream"
	}
	return t
}

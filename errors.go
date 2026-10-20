package stackhive

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// The cases a caller is likely to want to tell apart. Test for them with
// errors.Is, which also matches the APIError values the service returns:
//
//	if errors.Is(err, stackhive.ErrUnauthorized) { ... }
var (
	// ErrUnauthorized means the API key is missing, wrong, revoked or
	// expired, or its owner has left the key's workspace (code
	// "invalid_key"). Retrying will not help.
	ErrUnauthorized = errors.New("stackhive: API key rejected")

	// ErrForbidden means the key may not do this. Its access level may not
	// include it (code "insufficient_scope", e.g. a read-only key uploading),
	// the route may not be open to API keys ("api_key_not_allowed"), or the
	// key's owner may not be allowed it in the workspace.
	ErrForbidden = errors.New("stackhive: not allowed for this API key")

	// ErrNotFiled means an upload's bytes were stored but the file could not
	// be added to the workspace, for example because its folder was deleted
	// during the upload. The APIError's message says why.
	ErrNotFiled = errors.New("stackhive: uploaded but not added to the workspace")

	// ErrNotFound means no such object, or it belongs to someone else. The
	// service deliberately does not say which: admitting an object exists but
	// is not yours leaks that it exists.
	ErrNotFound = errors.New("stackhive: not found")

	// ErrExpired means the upload token timed out before the transfer
	// finished. The parts already stored are orphaned; upload the file again.
	ErrExpired = errors.New("stackhive: upload token expired; upload the file again")

	// ErrCancelled means Cancel was called while the upload was running.
	ErrCancelled = errors.New("stackhive: upload cancelled")
)

// APIError is a response the service sent back with a non-success status.
type APIError struct {
	Status  int    // HTTP status
	Code    string // the service's machine-readable code, e.g. "offset_mismatch"
	Message string // the human-readable explanation
}

// Error formats the failure for a log line.
func (e *APIError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("stackhive: %d %s: %s", e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("stackhive: %d: %s", e.Status, e.Message)
}

// Is lets errors.Is match an APIError against the sentinel errors above.
//
// Callers then write errors.Is(err, ErrNotFound) without caring whether the
// error came from a status code or from somewhere inside the SDK.
func (e *APIError) Is(target error) bool {
	switch target {
	case ErrUnauthorized:
		return e.Status == http.StatusUnauthorized
	case ErrForbidden:
		return e.Status == http.StatusForbidden
	case ErrNotFound:
		return e.Status == http.StatusNotFound
	case ErrNotFiled:
		return e.Status == http.StatusUnprocessableEntity && e.Code == "not_filed"
	}
	return false
}

// Retryable reports whether the same call could plausibly succeed later.
//
// A rate limit or a 5xx is the service's problem rather than the request's;
// everything else will fail the same way again.
func (e *APIError) Retryable() bool {
	return e.Status == http.StatusTooManyRequests || e.Status >= 500
}

// newAPIError builds an APIError from a failed response body.
//
// It falls back to the raw body because a proxy or load balancer may answer
// with HTML instead of the service's JSON, and seeing that is more useful than
// seeing nothing. The fallback is trimmed so a large page cannot flood a log.
func newAPIError(status int, body []byte) *APIError {
	e := &APIError{Status: status}

	var payload struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &payload) == nil && payload.Message != "" {
		e.Code, e.Message = payload.Code, payload.Message
		return e
	}

	text := strings.TrimSpace(string(body))
	if len(text) > 200 {
		text = text[:200] + "…"
	}
	if text == "" {
		text = http.StatusText(status)
	}
	e.Message = text
	return e
}

// TransferFailedError means the SDK ran out of attempts.
//
// It is worth trying again later: the bytes already stored survive, so a new
// attempt on the same Upload resumes rather than restarting.
type TransferFailedError struct {
	Attempts         int   // attempts made without progress before giving up
	BytesTransferred int64 // how far the service confirmed it got
	Err              error // the last failure seen
}

// Error formats the failure, including how far it got — a number that never
// moves across retries points at a broken network path, one that creeps up
// points at a slow or flaky one.
func (e *TransferFailedError) Error() string {
	return fmt.Sprintf("stackhive: gave up after %d attempts at byte %d: %v",
		e.Attempts, e.BytesTransferred, e.Err)
}

// Unwrap exposes the last underlying failure to errors.Is and errors.As.
func (e *TransferFailedError) Unwrap() error {
	return e.Err
}

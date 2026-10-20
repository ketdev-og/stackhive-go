package stackhive

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func testClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := New(Config{BaseURL: server.URL, APIKey: "ak_test_0123456789abcdef_secret"})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// The key's workspace is asked for once, and ListFiles sends its options as
// the list endpoint's query.
func TestListFilesUsesTheKeysWorkspace(t *testing.T) {
	var keyCalls atomic.Int32
	var listQuery string
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/storage/key":
			keyCalls.Add(1)
			_, _ = w.Write([]byte(`{"keyId":"key-1","workspaceId":"ws-1","accessLevel":"read_only"}`))
		case "/api/storage/workspaces/ws-1/assets":
			listQuery = r.URL.RawQuery
			_, _ = w.Write([]byte(`{"items":[{"id":"f-1","type":"file","name":"a.pdf","sizeBytes":3}],"page":1,"pageSize":20,"total":1,"totalPages":1}`))
		default:
			http.NotFound(w, r)
		}
	})

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		page, err := client.ListFiles(ctx, ListOptions{FolderID: "folder-1", Recursive: true, Query: "a", MimeTypes: []string{"image/png", "application/pdf"}, PageSize: 50})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Files) != 1 || page.Files[0].Name != "a.pdf" || page.Files[0].IsFolder() {
			t.Fatalf("page = %+v", page)
		}
	}
	if keyCalls.Load() != 1 {
		t.Fatalf("the key was asked for %d times, want once", keyCalls.Load())
	}
	want := "mimeType=image%2Fpng%2Capplication%2Fpdf&pageSize=50&parentId=folder-1&q=a&recursive=true"
	if listQuery != want {
		t.Fatalf("query = %s, want %s", listQuery, want)
	}
}

// A rate-limited or busy answer is tried again, after its Retry-After.
func TestCallRetriesBusyAnswers(t *testing.T) {
	var calls atomic.Int32
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"objectId":"o-1","status":"completed"}`))
	})
	obj, err := client.Get(context.Background(), "o-1")
	if err != nil || obj.ID != "o-1" || calls.Load() != 3 {
		t.Fatalf("get = %+v, %v after %d calls; want o-1 after 3", obj, err, calls.Load())
	}
}

func TestErrorsMatchTheirCauses(t *testing.T) {
	for _, tt := range []struct {
		status int
		body   string
		want   error
		not    error
	}{
		{401, `{"code":"invalid_key","message":"bad key"}`, ErrUnauthorized, ErrForbidden},
		{403, `{"code":"insufficient_scope","message":"This API key can't upload files"}`, ErrForbidden, ErrUnauthorized},
		{404, `{"code":"not_found","message":"No such object."}`, ErrNotFound, ErrForbidden},
		{422, `{"code":"not_filed","message":"folder gone"}`, ErrNotFiled, ErrNotFound},
	} {
		err := newAPIError(tt.status, []byte(tt.body))
		if !errors.Is(err, tt.want) || errors.Is(err, tt.not) {
			t.Errorf("%d %s: matches %v = %v, %v = %v", tt.status, tt.body, tt.want, errors.Is(err, tt.want), tt.not, errors.Is(err, tt.not))
		}
	}
}

func TestRetryDelay(t *testing.T) {
	if got := retryDelay("2", 0); got != 2*time.Second {
		t.Errorf("Retry-After 2 = %v", got)
	}
	if got := retryDelay("3600", 0); got != time.Minute {
		t.Errorf("Retry-After 3600 = %v, want capped at a minute", got)
	}
	if got := retryDelay("", 1); got != backoff(1) {
		t.Errorf("no Retry-After = %v, want the backoff", got)
	}
}

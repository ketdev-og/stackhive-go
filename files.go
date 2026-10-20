package stackhive

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// KeyInfo is what the service says about the client's API key.
type KeyInfo struct {
	ID            string `json:"keyId"`
	WorkspaceID   string `json:"workspaceId"`
	WorkspaceName string `json:"workspaceName"`

	// AccessLevel is "read_only" (view and download), "upload_only" (upload
	// and create folders) or "read_write" (all of those, plus rename and
	// delete). Permissions lists the same as permission names.
	AccessLevel string   `json:"accessLevel"`
	Permissions []string `json:"permissions"`

	// UploadFolderID is where uploads that name no folder go; empty for the
	// top level. UploadFolderName is empty for the top level, or when that
	// folder has been deleted, in which case such uploads are refused.
	UploadFolderID   string `json:"uploadFolderId"`
	UploadFolderName string `json:"uploadFolderName"`
}

// Key describes the client's API key: its workspace, access level and upload
// folder. It is fetched once and remembered, since a key's workspace never
// changes.
func (c *Client) Key(ctx context.Context) (*KeyInfo, error) {
	c.keyMu.Lock()
	defer c.keyMu.Unlock()
	if c.key != nil {
		return c.key, nil
	}
	var key KeyInfo
	if err := c.call(ctx, http.MethodGet, "/api/storage/key", nil, &key); err != nil {
		return nil, err
	}
	c.key = &key
	return c.key, nil
}

// File is a file or folder in the API key's workspace, as people see it in
// the app.
type File struct {
	ID string `json:"id"`
	// Type is "file" or "folder".
	Type string `json:"type"`
	Name string `json:"name"`
	// FolderID is the folder it is in; empty at the top level.
	FolderID string `json:"parentId"`
	// Size and MimeType are empty for a folder.
	Size     int64  `json:"sizeBytes"`
	MimeType string `json:"mimeType"`
	// CreatedBy is the name of the person who added it.
	CreatedBy string    `json:"createdByName"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// IsFolder reports whether f is a folder.
func (f File) IsFolder() bool { return f.Type == "folder" }

// ListOptions narrows ListFiles. The zero value lists the top level.
type ListOptions struct {
	// FolderID lists inside this folder; empty for the top level.
	FolderID string
	// Recursive lists everything below the folder, not only what is directly
	// in it.
	Recursive bool
	// Query keeps names containing this text (case-insensitive).
	Query string
	// Type keeps only "file" or "folder".
	Type string
	// MimeTypes keeps files of these types, e.g. "image/png".
	MimeTypes []string
	// Page starts at 1. PageSize is 1 to 100; default 20.
	Page     int
	PageSize int
}

// FilePage is one page of ListFiles, sorted by name.
type FilePage struct {
	Files      []File `json:"items"`
	Page       int    `json:"page"`
	PageSize   int    `json:"pageSize"`
	Total      int    `json:"total"`
	TotalPages int    `json:"totalPages"`
}

// ListFiles lists files and folders in the key's workspace. The key's access
// level must allow viewing files.
func (c *Client) ListFiles(ctx context.Context, opts ListOptions) (*FilePage, error) {
	query := url.Values{}
	set := func(name, value string) {
		if value != "" {
			query.Set(name, value)
		}
	}
	set("parentId", opts.FolderID)
	set("q", opts.Query)
	set("type", opts.Type)
	set("mimeType", strings.Join(opts.MimeTypes, ","))
	if opts.Recursive {
		query.Set("recursive", "true")
	}
	if opts.Page > 0 {
		query.Set("page", strconv.Itoa(opts.Page))
	}
	if opts.PageSize > 0 {
		query.Set("pageSize", strconv.Itoa(opts.PageSize))
	}
	path, err := c.workspacePath(ctx, "/assets")
	if err != nil {
		return nil, err
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var page FilePage
	if err := c.call(ctx, http.MethodGet, path, nil, &page); err != nil {
		return nil, err
	}
	return &page, nil
}

// GetFile fetches one file or folder.
func (c *Client) GetFile(ctx context.Context, fileID string) (*File, error) {
	return c.fileCall(ctx, http.MethodGet, fileID, nil)
}

// CreateFolder makes a folder called name inside parentID, or at the top
// level when parentID is empty. A folder of that name already there is an
// APIError with status 409. The key's access level must allow uploads.
func (c *Client) CreateFolder(ctx context.Context, name, parentID string) (*File, error) {
	path, err := c.workspacePath(ctx, "/assets")
	if err != nil {
		return nil, err
	}
	body := map[string]any{"type": "folder", "name": name}
	if parentID != "" {
		body["parentId"] = parentID
	}
	var folder File
	if err := c.call(ctx, http.MethodPost, path, body, &folder); err != nil {
		return nil, err
	}
	return &folder, nil
}

// RenameFile renames a file or folder. The key's access level must allow
// renaming (read_write).
func (c *Client) RenameFile(ctx context.Context, fileID, name string) (*File, error) {
	return c.fileCall(ctx, http.MethodPatch, fileID, map[string]any{"name": name})
}

// DeleteFile deletes a file, or a folder with everything in it. The key's
// access level must allow deleting (read_write).
func (c *Client) DeleteFile(ctx context.Context, fileID string) error {
	path, err := c.workspacePath(ctx, "/assets/"+url.PathEscape(fileID))
	if err != nil {
		return err
	}
	return c.call(ctx, http.MethodDelete, path, nil, nil)
}

// DownloadFile fetches any file in the key's workspace into target, resuming
// from whatever is already on disk, like DownloadTo. The key's access level
// must allow downloads.
func (c *Client) DownloadFile(ctx context.Context, fileID, target string, opts ...Option) error {
	return c.download(ctx, target, opts, func(ctx context.Context) (string, error) {
		path, err := c.workspacePath(ctx, "/assets/"+url.PathEscape(fileID)+"/download")
		if err != nil {
			return "", err
		}
		var response struct {
			URL  string `json:"url"`
			Mock bool   `json:"mock"`
		}
		if err := c.call(ctx, http.MethodGet, path, nil, &response); err != nil {
			return "", err
		}
		if response.Mock || response.URL == "" {
			return "", errors.New("stackhive: this file has no stored content to download")
		}
		return response.URL, nil
	})
}

func (c *Client) fileCall(ctx context.Context, method, fileID string, body any) (*File, error) {
	path, err := c.workspacePath(ctx, "/assets/"+url.PathEscape(fileID))
	if err != nil {
		return nil, err
	}
	var file File
	if err := c.call(ctx, method, path, body, &file); err != nil {
		return nil, err
	}
	return &file, nil
}

// workspacePath is suffix under the key's workspace in the storage API.
func (c *Client) workspacePath(ctx context.Context, suffix string) (string, error) {
	key, err := c.Key(ctx)
	if err != nil {
		return "", err
	}
	return "/api/storage/workspaces/" + url.PathEscape(key.WorkspaceID) + suffix, nil
}

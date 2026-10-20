package stackhive

import "time"

// StoredObject is a file held by the service.
//
// ID is the only field worth keeping — everything else can be read back from it
// with Client.Get.
type StoredObject struct {
	ID        string    `json:"objectId"`
	Filename  string    `json:"filename"`
	Mime      string    `json:"mime"`
	Size      int64     `json:"size"`
	Status    string    `json:"status"` // uploading, completed or aborted
	ETag      string    `json:"etag"`
	CreatedAt time.Time `json:"createdAt"`

	// AssetID is the workspace file a finished upload became, for GetFile,
	// DownloadFile, RenameFile and DeleteFile. SavedAs is its name: the
	// file's own, or "name (1).ext" when that was taken in its folder.
	AssetID string `json:"assetId"`
	SavedAs string `json:"savedAs"`
}

// IsComplete reports whether the file is fully stored and can be downloaded.
//
// It saves callers from comparing against a status vocabulary that belongs to
// the service rather than to them.
func (o StoredObject) IsComplete() bool {
	return o.Status == "completed"
}

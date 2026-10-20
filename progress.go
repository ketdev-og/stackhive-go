package stackhive

// ProgressFunc is called as bytes move, so a caller can draw a progress bar.
//
// It runs on the goroutine doing the transfer, so keep it quick — anything slow
// here slows the transfer down. On a resumed upload transferred starts at the
// offset the service already holds, not at zero, so a bar carries on instead of
// jumping back. total is -1 when a download's size is not known.
type ProgressFunc func(transferred, total int64)

// Option adjusts a single upload or download.
type Option func(*options)

type options struct {
	progress   ProgressFunc
	parentID   string
	assetName  string
	folderPath string
}

// WithProgress reports progress to fn as the transfer runs.
func WithProgress(fn ProgressFunc) Option {
	return func(o *options) { o.progress = fn }
}

// WithParentID places the uploaded file in this folder. Without it the file
// goes to the API key's upload folder, or the top level when it has none.
func WithParentID(id string) Option {
	return func(o *options) { o.parentID = id }
}

// WithFolderPath files the upload in these folders, e.g. "2026/oct",
// creating any that are missing. It starts in the WithParentID folder, else
// the API key's upload folder, else the top level.
func WithFolderPath(path string) Option {
	return func(o *options) { o.folderPath = path }
}

// WithAssetName names the uploaded file. Defaults to the file's name. A name
// already taken in the folder gets " (1)" added, and so on.
func WithAssetName(name string) Option {
	return func(o *options) { o.assetName = name }
}

// collect applies the options to a fresh set of defaults.
//
// A no-op progress function is filled in so the transfer code never has to
// check for nil on every chunk.
func collect(opts []Option) options {
	o := options{progress: func(int64, int64) {}}
	for _, apply := range opts {
		if apply != nil {
			apply(&o)
		}
	}
	if o.progress == nil {
		o.progress = func(int64, int64) {}
	}
	return o
}

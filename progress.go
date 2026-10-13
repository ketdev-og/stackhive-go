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
	progress ProgressFunc
	parentID  string
	assetName string
}

// WithProgress reports progress to fn as the transfer runs.
func WithProgress(fn ProgressFunc) Option {
	return func(o *options) { o.progress = fn }
}

// WithParentID places the created asset under this folder. Omit for the root.
func WithParentID(id string) Option {
	return func(o *options) {o.parentID = id}
}

// WithAssetName names the created asset. Defaults to the file's name.
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

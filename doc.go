// Package stackhive is the Go client for the stack_hive document and media
// storage service.
//
// Build one client and share it — it is safe for concurrent use and holds a
// connection pool:
//
//	client, err := stackhive.New(stackhive.Config{
//		BaseURL: "https://api.example.com",
//		APIKey:  os.Getenv("STACKHIVE_API_KEY"),
//	})
//
//	obj, err := client.UploadFile(ctx, "invoices-2026.zip")
//	err = client.DownloadTo(ctx, obj.ID, "/tmp/out.zip")
//
// Uploads resume by themselves. If the connection drops, the client asks the
// service how much it already holds and sends only the rest. The caller never
// computes an offset or sees a part.
//
// For pause, resume, cancel or progress, take a handle with NewUpload instead
// of calling UploadFile.
//
// The package has no dependencies outside the standard library.
package stackhive

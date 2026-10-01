# stackhive-go

Go client for the stack_hive document and media storage service.

```bash
go get github.com/ketdev-og/stackhive-go
```

```go
client, err := stackhive.New(stackhive.Config{
    BaseURL: "https://api.example.com",
    APIKey:  os.Getenv("STACKHIVE_API_KEY"),
})

obj, err := client.UploadFile(ctx, "invoices-2026.zip")
err = client.DownloadTo(ctx, obj.ID, "/tmp/out.zip")
```

Uploads resume by themselves if the connection drops. For progress, pause,
resume or cancel, use `client.NewUpload`.

Go 1.21 or newer. No dependencies outside the standard library.

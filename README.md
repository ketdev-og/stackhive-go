# stackhive-go

Go client for the stack_hive document and media storage service.

```bash
go get github.com/ketdev-og/stackhive-go
```

Go 1.21 or newer. No dependencies outside the standard library.

## API keys

Create a key in the app for one workspace. Each key has an **access level**:

| Level | Can |
| --- | --- |
| `read_only` | list and view files, download |
| `upload_only` | upload, create folders |
| `read_write` | all of the above, plus rename and delete |

A key never does more than the person who created it may do in that
workspace, and stops working while they are out of it. It may also have an
**upload folder**: where uploads that name no folder go.

```go
client, err := stackhive.New(stackhive.Config{
    BaseURL: "https://api.example.com",
    APIKey:  os.Getenv("STACKHIVE_API_KEY"),
})

key, err := client.Key(ctx) // workspace, access level, upload folder
```

## Uploading

```go
obj, err := client.UploadFile(ctx, "invoice-0042.pdf",
    stackhive.WithFolderPath("2026/oct"), // created if missing
)
fmt.Println(obj.AssetID, obj.SavedAs)
```

- Without options the file goes to the key's upload folder, or the top level.
- `WithParentID(folderID)` picks a folder; `WithFolderPath("a/b")` goes below
  it (or below the key's folder), creating folders that are missing.
- A name already taken in the folder is saved as `name (1).ext`; `SavedAs`
  tells you the name used. `AssetID` is the file in the workspace.
- Uploads resume by themselves if the connection drops. For progress, pause,
  resume or cancel, use `client.NewUpload`.

## Files in the workspace

```go
page, err := client.ListFiles(ctx, stackhive.ListOptions{
    FolderID:  folderID,  // empty: the top level
    Recursive: true,      // everything below, not just direct children
    MimeTypes: []string{"application/pdf"},
})
file, err := client.GetFile(ctx, fileID)
err = client.DownloadFile(ctx, fileID, "/tmp/invoice.pdf") // resumes
folder, err := client.CreateFolder(ctx, "Receipts", parentID)
file, err = client.RenameFile(ctx, fileID, "invoice-final.pdf")
err = client.DeleteFile(ctx, fileID)
```

`List`, `Get` and `DownloadTo` still work with the key's own uploads by object ID.

## Errors

Test with `errors.Is`:

| Error | Means |
| --- | --- |
| `ErrUnauthorized` | wrong, expired or revoked key, or its owner left the workspace |
| `ErrForbidden` | the key's access level (`insufficient_scope`) or its owner doesn't allow this |
| `ErrNotFound` | no such file, or not in the key's workspace |
| `ErrNotFiled` | an upload's bytes were stored but it couldn't be added to the workspace |
| `ErrExpired` | an upload took longer than its token allows; upload again |

Every failure from the service is an `*APIError` with `Status`, `Code` and
`Message`. Rate-limited (429) and busy (503) answers are retried, honouring
`Retry-After`.

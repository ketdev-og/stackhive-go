package stackhive_test

// These examples appear as documentation on pkg.go.dev. They are compiled by
// `go test` but not run, because they need a live service and an API key.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ketdev-og/stackhive-go"
)

// The whole job: one client, one upload, one download.
func Example() {
	client, err := stackhive.New(stackhive.Config{
		BaseURL: "https://api.example.com",
		APIKey:  os.Getenv("STACKHIVE_API_KEY"),
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	obj, err := client.UploadFile(ctx, "invoices-2026.zip")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("stored", obj.ID)

	if err := client.DownloadTo(ctx, obj.ID, "/tmp/out.zip"); err != nil {
		log.Fatal(err)
	}
}

// Progress, with pause and resume driven from another goroutine.
func ExampleUpload() {
	client, _ := stackhive.New(stackhive.Config{
		BaseURL: "https://api.example.com",
		APIKey:  os.Getenv("STACKHIVE_API_KEY"),
	})

	up, err := client.NewUpload("invoices-2026.zip",
		stackhive.WithProgress(func(sent, total int64) {
			fmt.Printf("\r%.1f%%", 100*float64(sent)/float64(total))
		}))
	if err != nil {
		log.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		obj, err := up.Start(context.Background())
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("\nstored", obj.ID)
	}()

	time.Sleep(5 * time.Second)
	up.Pause()
	time.Sleep(5 * time.Second)
	up.Resume()

	<-done
}

// Telling failures apart.
func ExampleClient_UploadFile_errors() {
	client, _ := stackhive.New(stackhive.Config{
		BaseURL: "https://api.example.com",
		APIKey:  os.Getenv("STACKHIVE_API_KEY"),
	})

	_, err := client.UploadFile(context.Background(), "invoices-2026.zip")

	var transfer *stackhive.TransferFailedError
	switch {
	case err == nil:
		fmt.Println("stored")
	case errors.Is(err, stackhive.ErrUnauthorized):
		fmt.Println("check the API key")
	case errors.Is(err, stackhive.ErrExpired):
		fmt.Println("took too long; upload again")
	case errors.As(err, &transfer):
		fmt.Println("network trouble; got as far as", transfer.BytesTransferred)
	default:
		fmt.Println("failed:", err)
	}
}

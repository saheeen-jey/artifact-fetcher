package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/example/artifact-fetcher/downloader"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "download":
		download(os.Args[2:])
	case "verify":
		verify(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func download(args []string) {
	flags := flag.NewFlagSet("download", flag.ExitOnError)
	output := flags.String("output", "", "final artifact path")
	connections := flags.Int("connections", 4, "maximum concurrent connections")
	chunkSize := flags.Int64("chunk-size", 8<<20, "chunk size in bytes")
	resume := flags.Bool("resume", false, "resume from a persisted manifest")
	checksum := flags.String("sha256", "", "expected SHA-256 checksum")
	retries := flags.Int("retries", 3, "maximum retries per chunk")
	jsonOutput := flags.Bool("json", false, "emit progress as JSON")
	flags.Parse(args)
	if flags.NArg() != 1 || *output == "" {
		fmt.Fprintln(os.Stderr, "usage: artifact-fetcher download URL --output FILE")
		os.Exit(2)
	}
	d := downloader.Downloader{Connections: *connections, ChunkSize: *chunkSize, Resume: *resume, Retries: *retries}
	d.Progress = func(progress downloader.Progress) {
		if *jsonOutput {
			_ = json.NewEncoder(os.Stdout).Encode(progress)
			return
		}
		fmt.Printf("completed chunk %d/%d (%d/%d bytes)\n", progress.Chunk+1, progress.Chunks, progress.Bytes, progress.Total)
	}
	if err := d.Download(context.Background(), downloader.Options{URL: flags.Arg(0), Output: *output, Checksum: *checksum}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func verify(args []string) {
	flags := flag.NewFlagSet("verify", flag.ExitOnError)
	checksum := flags.String("sha256", "", "expected SHA-256 checksum")
	flags.Parse(args)
	if flags.NArg() != 1 || *checksum == "" {
		fmt.Fprintln(os.Stderr, "usage: artifact-fetcher verify FILE --sha256 HASH")
		os.Exit(2)
	}
	file, err := os.Open(flags.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		file.Close()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	file.Close()
	actual := hex.EncodeToString(hash.Sum(nil))
	if actual != *checksum {
		fmt.Fprintf(os.Stderr, "checksum mismatch: got %s, want %s\n", actual, *checksum)
		os.Exit(1)
	}
	fmt.Println("checksum verified")
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: artifact-fetcher download URL --output FILE | verify FILE --sha256 HASH")
}

package main

import (
	"context"
	"flag"
	"fmt"
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
	flags.Parse(args)
	if flags.NArg() != 1 || *output == "" {
		fmt.Fprintln(os.Stderr, "usage: artifact-fetcher download URL --output FILE")
		os.Exit(2)
	}
	d := downloader.Downloader{Connections: *connections, ChunkSize: *chunkSize}
	if err := d.Download(context.Background(), downloader.Options{URL: flags.Arg(0), Output: *output}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: artifact-fetcher download URL --output FILE")
}
package downloader

import (
	"context"
	"errors"
	"net/http"
)

type Downloader struct {
	Client      *http.Client
	Connections int
	ChunkSize   int64
}

type Options struct {
	URL    string
	Output string
}

type Chunk struct {
	Index int
	Start int64
	End   int64
}

func (d *Downloader) Download(ctx context.Context, opts Options) error {
	if opts.URL == "" || opts.Output == "" {
		return errors.New("URL and output are required")
	}
	client := d.Client
	if client == nil {
		client = http.DefaultClient
	}
	remote, err := discover(ctx, client, opts.URL)
	if err != nil {
		return err
	}
	_ = planChunks(remote.Size, d.ChunkSize)
	return errors.New("download implementation is not initialized")
}

func planChunks(size, chunkSize int64) []Chunk {
	if chunkSize <= 0 || chunkSize > size {
		chunkSize = size
	}
	if size == 0 {
		return nil
	}
	chunks := make([]Chunk, 0, (size+chunkSize-1)/chunkSize)
	for index, start := 0, int64(0); start < size; index, start = index+1, start+chunkSize {
		end := start + chunkSize - 1
		if end >= size {
			end = size - 1
		}
		chunks = append(chunks, Chunk{Index: index, Start: start, End: end})
	}
	return chunks
}
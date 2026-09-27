package downloader

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
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
	connections := d.Connections
	if connections <= 0 {
		connections = 1
	}
	chunks := planChunks(remote.Size, d.ChunkSize)
	temporary := opts.Output + ".part"
	if err := os.MkdirAll(filepath.Dir(temporary), 0755); err != nil {
		return err
	}
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := file.Truncate(remote.Size); err != nil {
		return err
	}

	jobs := make(chan Chunk)
	var workers sync.WaitGroup
	var firstErr error
	var errorMu sync.Mutex
	workerCount := connections
	if workerCount > len(chunks) && len(chunks) > 0 {
		workerCount = len(chunks)
	}
	for index := 0; index < workerCount; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for chunk := range jobs {
				if err := downloadChunk(ctx, client, remote, file, chunk); err != nil {
					errorMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errorMu.Unlock()
					return
				}
			}
		}()
	}
	for _, chunk := range chunks {
		errorMu.Lock()
		err = firstErr
		errorMu.Unlock()
		if err != nil {
			break
		}
		select {
		case jobs <- chunk:
		case <-ctx.Done():
			errorMu.Lock()
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			errorMu.Unlock()
		}
	}
	close(jobs)
	workers.Wait()
	if firstErr != nil {
		return firstErr
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(temporary, opts.Output)
}

func downloadChunk(ctx context.Context, client *http.Client, remote RemoteFile, file *os.File, chunk Chunk) error {
	body, err := requestRange(ctx, client, remote, chunk.Start, chunk.End)
	if err != nil {
		return err
	}
	defer body.Close()
	buffer := make([]byte, 128*1024)
	position := chunk.Start
	for position <= chunk.End {
		read, readErr := body.Read(buffer)
		if read > 0 {
			written, writeErr := file.WriteAt(buffer[:read], position)
			if writeErr != nil {
				return writeErr
			}
			if written != read {
				return errors.New("short write while storing chunk")
			}
			position += int64(read)
			if position > chunk.End+1 {
				return errors.New("range response exceeded requested size")
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return readErr
		}
	}
	if position != chunk.End+1 {
		return errors.New("range response was shorter than requested")
	}
	return nil
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

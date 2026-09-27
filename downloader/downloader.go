package downloader

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Downloader struct {
	Client      *http.Client
	Connections int
	ChunkSize   int64
	Resume      bool
	Retries     int
	Progress    func(Progress)
}

type Options struct {
	URL      string
	Output   string
	Checksum string
}

type Progress struct {
	Chunk           int
	Chunks          int
	Bytes           int64
	Total           int64
	Retries         int
	Completed       bool
	Reused          bool
	CompletedChunks int
	ReusedChunks    int
	RetriedChunks   int
}

var ErrChecksumMismatch = errors.New("checksum mismatch")

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
	manifestPath := temporary + ".json"
	if err := os.MkdirAll(filepath.Dir(temporary), 0755); err != nil {
		return err
	}
	state := manifest{URL: remote.URL, Size: remote.Size, ChunkSize: d.ChunkSize, Chunks: make([]bool, len(chunks))}
	if d.Resume {
		if existing, loadErr := loadManifest(manifestPath); loadErr == nil {
			if existing.URL != state.URL || existing.Size != state.Size || existing.ChunkSize != state.ChunkSize || len(existing.Chunks) != len(state.Chunks) {
				return errors.New("resume manifest does not match the remote file")
			}
			state = existing
		}
	} else {
		_ = os.Remove(manifestPath)
	}
	flags := os.O_CREATE | os.O_RDWR
	if !d.Resume {
		flags |= os.O_TRUNC
	}
	file, err := os.OpenFile(temporary, flags, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := file.Truncate(remote.Size); err != nil {
		return err
	}
	if err := saveManifest(manifestPath, state); err != nil {
		return err
	}

	jobs := make(chan Chunk)
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup
	var firstErr error
	var errorMu sync.Mutex
	var stateMu sync.Mutex
	completedBytes := int64(0)
	completedChunks := 0
	reusedChunks := 0
	retriedChunks := 0
	for index, complete := range state.Chunks {
		if complete {
			completedBytes += chunks[index].End - chunks[index].Start + 1
			completedChunks++
			reusedChunks++
		}
	}
	workerCount := connections
	if workerCount > len(chunks) && len(chunks) > 0 {
		workerCount = len(chunks)
	}
	for index := 0; index < workerCount; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for chunk := range jobs {
				stateMu.Lock()
				complete := state.Chunks[chunk.Index]
				progress := Progress{Chunk: chunk.Index, Chunks: len(chunks), Bytes: completedBytes, Total: remote.Size, Completed: complete, Reused: complete, CompletedChunks: completedChunks, ReusedChunks: reusedChunks, RetriedChunks: retriedChunks}
				stateMu.Unlock()
				if complete {
					if d.Progress != nil {
						d.Progress(progress)
					}
					continue
				}
				retries, err := retryChunk(workCtx, client, remote, file, chunk, d.Retries)
				if err != nil {
					errorMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errorMu.Unlock()
					cancel()
					return
				}
				stateMu.Lock()
				state.Chunks[chunk.Index] = true
				err = saveManifest(manifestPath, state)
				completedBytes += chunk.End - chunk.Start + 1
				completedChunks++
				if retries > 0 {
					retriedChunks++
				}
				progress = Progress{Chunk: chunk.Index, Chunks: len(chunks), Bytes: completedBytes, Total: remote.Size, Retries: retries, Completed: true, CompletedChunks: completedChunks, ReusedChunks: reusedChunks, RetriedChunks: retriedChunks}
				stateMu.Unlock()
				if err != nil {
					errorMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("save resume manifest: %w", err)
					}
					errorMu.Unlock()
					return
				}
				if d.Progress != nil {
					d.Progress(progress)
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
		case <-workCtx.Done():
			errorMu.Lock()
			if firstErr == nil && ctx.Err() != nil {
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
	if opts.Checksum != "" {
		actual, err := checksumFile(temporary)
		if err != nil {
			return err
		}
		if !strings.EqualFold(actual, opts.Checksum) {
			return fmt.Errorf("%w: got %s, want %s", ErrChecksumMismatch, actual, opts.Checksum)
		}
	}
	if err := os.Rename(temporary, opts.Output); err != nil {
		return err
	}
	return os.Remove(manifestPath)
}

func checksumFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func retryChunk(ctx context.Context, client *http.Client, remote RemoteFile, file *os.File, chunk Chunk, retries int) (int, error) {
	if retries < 0 {
		retries = 0
	}
	for attempt := 0; ; attempt++ {
		err := downloadChunk(ctx, client, remote, file, chunk)
		if err == nil || attempt >= retries || !retryable(err) {
			return attempt, err
		}
		backoff := time.Duration(1<<min(attempt, 5)) * 100 * time.Millisecond
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return attempt, ctx.Err()
		}
	}
}

func retryable(err error) bool {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode == http.StatusRequestTimeout || httpErr.StatusCode == http.StatusTooManyRequests || httpErr.StatusCode >= 500
	}
	return true
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
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

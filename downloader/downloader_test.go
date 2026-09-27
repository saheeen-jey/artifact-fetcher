package downloader

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestPlanChunks(t *testing.T) {
	chunks := planChunks(10, 4)
	want := []Chunk{{Index: 0, Start: 0, End: 3}, {Index: 1, Start: 4, End: 7}, {Index: 2, Start: 8, End: 9}}
	if len(chunks) != len(want) {
		t.Fatalf("got %d chunks, want %d", len(chunks), len(want))
	}
	for index := range want {
		if chunks[index] != want[index] {
			t.Errorf("chunk %d = %#v, want %#v", index, chunks[index], want[index])
		}
	}
}

func TestPlanChunksEmptyFile(t *testing.T) {
	if chunks := planChunks(0, 4); chunks != nil {
		t.Fatalf("got %#v, want nil", chunks)
	}
}

func TestDownloadPublishesCompleteRangeFile(t *testing.T) {
	data := []byte("abcdefghijklmnopqrstuvwxyz0123456789")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodHead {
			writer.Header().Set("Accept-Ranges", "bytes")
			writer.Header().Set("Content-Length", fmt.Sprint(len(data)))
			writer.WriteHeader(http.StatusOK)
			return
		}
		start, end := int64(0), int64(len(data)-1)
		if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
			http.Error(writer, "missing range", http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		writer.WriteHeader(http.StatusPartialContent)
		_, _ = writer.Write(data[start : end+1])
	}))
	defer server.Close()

	output := filepath.Join(t.TempDir(), "artifact.bin")
	d := Downloader{Client: server.Client(), Connections: 3, ChunkSize: 7}
	if err := d.Download(context.Background(), Options{URL: server.URL, Output: output}); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if string(actual) != string(data) {
		t.Fatalf("downloaded %q, want %q", actual, data)
	}
	if _, err := os.Stat(output + ".part"); !os.IsNotExist(err) {
		t.Fatalf("temporary file still exists: %v", err)
	}
}

func TestDownloadResumesCompletedChunks(t *testing.T) {
	data := []byte("abcdefghijklmnopqrstuvwxyz0123456789")
	var mu sync.Mutex
	failed := false
	rangeRequests := make(map[int64]int)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodHead {
			writer.Header().Set("Accept-Ranges", "bytes")
			writer.Header().Set("Content-Length", fmt.Sprint(len(data)))
			return
		}
		var start, end int64
		if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
			http.Error(writer, "missing range", http.StatusBadRequest)
			return
		}
		mu.Lock()
		rangeRequests[start]++
		shouldFail := start == 14 && !failed
		if shouldFail {
			failed = true
		}
		mu.Unlock()
		if shouldFail {
			http.Error(writer, "temporary failure", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusPartialContent)
		_, _ = writer.Write(data[start : end+1])
	}))
	defer server.Close()

	output := filepath.Join(t.TempDir(), "artifact.bin")
	first := Downloader{Client: server.Client(), Connections: 1, ChunkSize: 7, Retries: 0}
	if err := first.Download(context.Background(), Options{URL: server.URL, Output: output}); err == nil {
		t.Fatal("first download unexpectedly succeeded")
	}
	second := Downloader{Client: server.Client(), Connections: 1, ChunkSize: 7, Resume: true, Retries: 0}
	if err := second.Download(context.Background(), Options{URL: server.URL, Output: output}); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if string(actual) != string(data) {
		t.Fatalf("downloaded %q, want %q", actual, data)
	}
	mu.Lock()
	defer mu.Unlock()
	if rangeRequests[0] != 1 || rangeRequests[7] != 1 {
		t.Fatalf("completed ranges were redownloaded: %#v", rangeRequests)
	}
}

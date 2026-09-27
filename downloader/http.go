package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

var ErrRangeUnsupported = errors.New("server does not support byte ranges")

type RemoteFile struct {
	URL         string
	Size        int64
	AcceptRanges bool
}

func discover(ctx context.Context, client *http.Client, rawURL string) (RemoteFile, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURL, nil)
	if err != nil {
		return RemoteFile{}, fmt.Errorf("create metadata request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return RemoteFile{}, fmt.Errorf("discover remote file: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return RemoteFile{}, fmt.Errorf("metadata request returned HTTP %d", resp.StatusCode)
	}
	if resp.ContentLength < 0 {
		return RemoteFile{}, errors.New("remote file has no Content-Length")
	}
	if !strings.EqualFold(resp.Header.Get("Accept-Ranges"), "bytes") {
		return RemoteFile{}, ErrRangeUnsupported
	}
	return RemoteFile{URL: rawURL, Size: resp.ContentLength, AcceptRanges: true}, nil
}

func requestRange(ctx context.Context, client *http.Client, remote RemoteFile, start, end int64) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, remote.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("create range request: %w", err)
	}
	req.Header.Set("Range", "bytes="+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10))
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		return nil, fmt.Errorf("range request returned HTTP %d", resp.StatusCode)
	}
	return resp.Body, nil
}
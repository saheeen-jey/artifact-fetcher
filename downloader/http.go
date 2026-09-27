package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

var ErrRangeUnsupported = errors.New("server does not support byte ranges")
var ErrPresignedURLExpired = errors.New("presigned URL was rejected or may be expired")

type RemoteFile struct {
	URL          string
	Size         int64
	AcceptRanges bool
}

type HTTPError struct {
	StatusCode int
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d", e.StatusCode)
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
		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotImplemented {
			return discoverWithRange(ctx, client, rawURL)
		}
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

func discoverWithRange(ctx context.Context, client *http.Client, rawURL string) (RemoteFile, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return RemoteFile{}, fmt.Errorf("create range metadata request: %w", err)
	}
	req.Header.Set("Range", "bytes=0-0")
	resp, err := client.Do(req)
	if err != nil {
		return RemoteFile{}, fmt.Errorf("discover remote file with range: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		if resp.StatusCode == http.StatusForbidden && isPresignedURL(rawURL) {
			return RemoteFile{}, fmt.Errorf("%w: generate a new presigned URL", ErrPresignedURLExpired)
		}
		return RemoteFile{}, fmt.Errorf("range metadata request returned HTTP %d", resp.StatusCode)
	}
	var start, end, size int64
	if _, err := fmt.Sscanf(resp.Header.Get("Content-Range"), "bytes %d-%d/%d", &start, &end, &size); err != nil || start != 0 || end != 0 || size < 1 {
		return RemoteFile{}, errors.New("range metadata response has no valid Content-Range")
	}
	return RemoteFile{URL: rawURL, Size: size, AcceptRanges: true}, nil
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
		if resp.StatusCode == http.StatusForbidden && isPresignedURL(remote.URL) {
			resp.Body.Close()
			return nil, fmt.Errorf("%w: generate a new presigned URL", ErrPresignedURLExpired)
		}
		resp.Body.Close()
		return nil, &HTTPError{StatusCode: resp.StatusCode}
	}
	return resp.Body, nil
}

func isPresignedURL(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	query := parsed.Query()
	return query.Get("X-Amz-Signature") != "" || query.Get("X-Amz-Credential") != ""
}

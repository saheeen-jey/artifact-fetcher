# artifact-fetcher

The download client that expects your network to fail.

`artifact-fetcher` is a Go CLI for downloading large artifacts from HTTP and S3-compatible storage through presigned URLs. It downloads byte ranges concurrently, persists completed chunks in a JSON manifest, retries transient failures, verifies SHA-256, and atomically publishes only verified files.

This is not positioned as a novel general-purpose downloader. Parallel HTTP downloading already exists in tools such as curl, Wget, aria2, rclone, and the AWS CLI. The project focuses on a narrower reliability workflow: crash-safe chunk state, cheap recovery after interruptions, presigned-URL support without embedded AWS credentials, fault-injection coverage, and verification before publication.

## Build

```powershell
go build -o artifact-fetcher.exe .\cmd\artifact-fetcher
```

## Download

```powershell
.\artifact-fetcher.exe download `
  --output .\large-file.iso `
  --connections 8 `
  --chunk-size 8388608 `
  --resume `
  --retries 3 `
  --sha256 EXPECTED_SHA256 `
  "https://example.com/large-file.iso"
```

The server must provide `Content-Length` and support byte ranges. The client uses `HEAD` metadata when available and falls back to a one-byte ranged `GET`, which supports S3 presigned URLs signed for `GET`. Interrupted downloads keep `.part` and `.part.json` beside the requested output. The final output is renamed into place only after all chunks and the optional checksum pass.

Use `--json` for one JSON progress object per completed chunk:

```powershell
.\artifact-fetcher.exe download URL --output .\artifact.bin --resume --json
```

Verify an existing file:

```powershell
.\artifact-fetcher.exe verify .\artifact.bin --sha256 EXPECTED_SHA256
```

## Private S3 objects

The MVP intentionally does not accept AWS credentials or `s3://` URLs. Generate a presigned URL with the AWS CLI and pass that URL unchanged to the downloader:

```powershell
aws s3 presign `
  s3://my-private-bucket/artifact.bin `
  --expires-in 3600 `
  --region us-east-1

.\artifact-fetcher.exe download "PRESIGNED_URL" `
  --output .\artifact.bin `
  --connections 8 `
  --resume `
  --sha256 EXPECTED_SHA256
```

If a presigned URL is rejected or expires during a transfer, the client reports that a new URL is required. The resume manifest currently binds the exact source URL, so remove the stale `.part` and manifest files before restarting with a newly generated URL.

## Development

```powershell
gofmt -w downloader cmd
go test ./...
go vet ./...
```

The integration tests use deterministic local HTTP servers to cover range placement, atomic publication, checksum failures, resume after a transient failure, dropped connections, corrupted responses, retry metrics, and expired presigned URLs.

## Scope

Direct S3 SDK support, distributed manifests, encryption, compression, and multipart S3 APIs are follow-up work. The project focuses on reliable artifact delivery in short-lived or unreliable compute environments.

# artifact-fetcher

The download client that expects your network to fail.

`artifact-fetcher` is a Go CLI for downloading large artifacts from HTTP and S3-compatible storage through presigned URLs. It downloads byte ranges concurrently, persists completed chunks in a JSON manifest, retries transient failures, verifies SHA-256, and atomically publishes only verified files.

## Build

```powershell
go build -o artifact-fetcher.exe .\cmd\artifact-fetcher
```

## Download

```powershell
.\artifact-fetcher.exe download `
  "https://example.com/large-file.iso" `
  --output .\large-file.iso `
  --connections 8 `
  --chunk-size 8388608 `
  --resume `
  --retries 3 `
  --sha256 EXPECTED_SHA256
```

The server must provide `Content-Length` and `Accept-Ranges: bytes`. Interrupted downloads keep `.part` and `.part.json` beside the requested output. The final output is renamed into place only after all chunks and the optional checksum pass.

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

If the presigned URL expires during a transfer, generate a new URL and resume while retaining the `.part` files.

## Development

```powershell
gofmt -w downloader cmd
go test ./...
go vet ./...
```

The integration tests use deterministic local HTTP servers to cover range placement, atomic publication, checksum failures, and resume after a transient failure.

## Scope

Direct S3 SDK support, distributed manifests, encryption, compression, and multipart S3 APIs are follow-up work. The project focuses on reliable artifact delivery in short-lived or unreliable compute environments.

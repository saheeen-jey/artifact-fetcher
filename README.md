# artifact-fetcher

The download client that expects your network to fail.

`artifact-fetcher` is a Go CLI for downloading large artifacts from HTTP and S3-compatible storage through presigned URLs. It downloads byte ranges concurrently, persists completed chunks in a JSON manifest, retries transient failures, verifies SHA-256, and atomically publishes only verified files.

This is not positioned as a novel general-purpose downloader. Parallel HTTP downloading already exists in tools such as curl, Wget, aria2, rclone, and the AWS CLI. The project focuses on a narrower reliability workflow: crash-safe chunk state, cheap recovery after interruptions, presigned-URL support without embedded AWS credentials, fault-injection coverage, and verification before publication.

**Status:** working, AWS-validated MVP. The project has been tested against a real private S3 object using a presigned URL. It does not currently include an AI model or AI-controlled recovery; AI-assisted diagnostics remain a future, explicitly bounded feature.

## The differentiator: a recovery receipt

The one feature this project deliberately develops beyond “another parallel downloader” is an explainable recovery receipt. The final progress event reports how many chunks were reused, how many needed retries, how many bytes were avoided after interruption, whether the checksum passed, and whether the verified file was published. This turns recovery from an opaque speed optimization into evidence a CI job can archive and audit. It is available as human-readable output or as a final JSON event with `--json`.

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

The final JSON object has `final: true` and acts as the recovery receipt. Its `bytes_avoided`, `reused_chunks`, `retried_chunks`, `checksum_verified`, and `published` fields are the primary reliability metrics.

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

If a presigned URL is rejected or expires during a transfer, the client reports that a new URL is required. Resume manifests bind to the stable object URL with `X-Amz-*` signing parameters removed, so a newly generated presigned URL for the same object can resume the incomplete chunks. New manifests do not store the temporary signature.

## Development

```powershell
gofmt -w downloader cmd
go test ./...
go vet ./...
```

The integration tests use deterministic local HTTP servers to cover range placement, atomic publication, checksum failures, resume after a transient failure, dropped connections, corrupted responses, retry metrics, and expired presigned URLs.

## Market boundary

| Tool | Strong at | artifact-fetcher’s deliberate focus |
| --- | --- | --- |
| aria2 | Broad protocols, segmented downloads, Metalink piece checksums, RPC | A small CI-oriented workflow for presigned URLs and recovery evidence |
| curl | Universal HTTP transfer, ranges, retries, rich transport diagnostics | Crash-safe chunk state and a publish-or-not recovery receipt |
| rclone | Many storage backends, sync semantics, multithreaded transfers | One large artifact, explicit chunk reuse, and verification before publication |
| AWS CLI | Native AWS authentication and S3 transfers | Credential-free presigned URL consumption with deterministic local failure tests |

The project should not claim to replace these tools generally. Its claim is narrower: it makes interrupted large artifact delivery measurable and cheap to recover.

## Credentials and security

The downloader does not accept, store, or manage AWS credentials. For private S3 objects, credentials stay with the AWS CLI or another trusted presigning workflow; `artifact-fetcher` receives only a temporary URL. Never commit access keys, secret keys, session tokens, passwords, or live presigned URLs.

New resume manifests store a normalized object identity rather than the full presigned URL, so temporary `X-Amz-*` signatures are not written to disk. The repository's `X-Amz-*` values are fake localhost test fixtures, not credentials.

The final artifact is written to a `.part` file and is renamed to the requested output only after all chunks and the optional checksum pass. Keep the AWS identity used for testing limited to the specific test bucket.

## GitHub publishing

The repository is intended to be published at:

```text
https://github.com/saheeen-jey/artifact-fetcher
```

Before pushing, scan the repository for credentials and verify the worktree:

```powershell
git status --short
git diff --check
git ls-files
```

Configure a GitHub remote and push using GitHub authentication already configured on your machine:

```powershell
git remote add origin https://github.com/saheeen-jey/artifact-fetcher.git
git branch -M main
git push -u origin main
```

Do not put a GitHub token in the remote URL. Use Git Credential Manager, GitHub CLI authentication, or SSH instead.

## Roadmap

- GitHub Actions CI and release packaging
- CGO-enabled race-detector validation
- Reusable fault-injection test server package
- Timing and throughput metrics in the recovery receipt
- Optional local AI diagnostics that analyze redacted failure events but never receive credentials or silently change download policy

## Scope

Direct S3 SDK support, distributed manifests, encryption, compression, and multipart S3 APIs are follow-up work. The project focuses on reliable artifact delivery in short-lived or unreliable compute environments.

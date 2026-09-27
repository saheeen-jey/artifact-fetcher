package downloader

import (
	"encoding/json"
	"fmt"
	"os"
)

type manifest struct {
	URL       string `json:"url"`
	Size      int64  `json:"size"`
	ChunkSize int64  `json:"chunk_size"`
	Chunks    []bool `json:"chunks"`
}

func loadManifest(path string) (manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return manifest{}, err
	}
	var state manifest
	if err := json.Unmarshal(data, &state); err != nil {
		return manifest{}, fmt.Errorf("read resume manifest: %w", err)
	}
	return state, nil
}

func saveManifest(path string, state manifest) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, 0600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

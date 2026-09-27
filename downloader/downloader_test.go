package downloader

import "testing"

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
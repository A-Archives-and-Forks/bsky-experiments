package repoarchive

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
)

// Index is an in-memory sorted index for a single .rca segment,
// supporting binary search by DID for random access.
type Index struct {
	Entries []IndexEntry
}

// LoadIndex reads the index section from a segment file.
// It reads the footer to locate the index, then reads and parses the index entries.
func LoadIndex(path string) (*Index, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}
	defer f.Close()

	// Read footer from end of file.
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat: %w", err)
	}
	if fi.Size() < FileHeaderSize+FileFooterSize {
		return nil, fmt.Errorf("file too small to be a valid segment")
	}

	if _, err := f.Seek(fi.Size()-FileFooterSize, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking to footer: %w", err)
	}
	footer, err := readFileFooter(f)
	if err != nil {
		return nil, fmt.Errorf("reading footer: %w", err)
	}

	// Read the index section.
	if _, err := f.Seek(footer.IndexOffset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking to index: %w", err)
	}
	indexData := make([]byte, footer.IndexSize)
	if _, err := io.ReadFull(f, indexData); err != nil {
		return nil, fmt.Errorf("reading index data: %w", err)
	}

	// Verify CRC.
	if checksumCRC32(indexData) != footer.CRC32 {
		return nil, fmt.Errorf("index CRC32 mismatch")
	}

	// Parse index entries.
	br := bytes.NewReader(indexData)
	count, err := readUint32(br)
	if err != nil {
		return nil, fmt.Errorf("reading entry count: %w", err)
	}

	entries := make([]IndexEntry, count)
	for i := range entries {
		did, err := readLenPrefixed(br)
		if err != nil {
			return nil, fmt.Errorf("reading DID at index %d: %w", i, err)
		}
		offset, err := readInt64(br)
		if err != nil {
			return nil, fmt.Errorf("reading offset at index %d: %w", i, err)
		}
		size, err := readUint32(br)
		if err != nil {
			return nil, fmt.Errorf("reading size at index %d: %w", i, err)
		}
		crc, err := readUint32(br)
		if err != nil {
			return nil, fmt.Errorf("reading CRC at index %d: %w", i, err)
		}
		entries[i] = IndexEntry{
			DID:    string(did),
			Offset: offset,
			Size:   size,
			CRC32:  crc,
		}
	}

	return &Index{Entries: entries}, nil
}

// FindDID performs a binary search for the given DID.
// Returns the IndexEntry and true if found, or a zero-value entry and false.
func (idx *Index) FindDID(did string) (IndexEntry, bool) {
	i := sort.Search(len(idx.Entries), func(i int) bool {
		return idx.Entries[i].DID >= did
	})
	if i < len(idx.Entries) && idx.Entries[i].DID == did {
		return idx.Entries[i], true
	}
	return IndexEntry{}, false
}

// DIDs returns all DIDs in the index (sorted).
func (idx *Index) DIDs() []string {
	dids := make([]string, len(idx.Entries))
	for i, e := range idx.Entries {
		dids[i] = e.DID
	}
	return dids
}

package repoarchive

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

// SegmentWriter writes CrawledRepo data into .rca segment files.
// It receives repos via a channel and writes them sequentially.
// When the current segment exceeds the target size, it finalizes
// the segment (writing index + footer) and opens a new one.
type SegmentWriter struct {
	outputDir   string
	segmentSize int64
	zstdLevel   int

	mu            sync.Mutex
	segmentNum    int
	file          *os.File
	offset        int64
	repoCount     uint32
	indexEntries  []IndexEntry
	encoder       *zstd.Encoder
	createdAt     time.Time
	bytesWritten  int64
	totalRepos    uint32
}

// WriterOption configures a SegmentWriter.
type WriterOption func(*SegmentWriter)

// WithSegmentSize sets the target segment file size.
func WithSegmentSize(size int64) WriterOption {
	return func(w *SegmentWriter) { w.segmentSize = size }
}

// WithZstdLevel sets the zstd compression level.
func WithZstdLevel(level int) WriterOption {
	return func(w *SegmentWriter) { w.zstdLevel = level }
}

// WithStartSegment sets the starting segment number (for resume).
func WithStartSegment(n int) WriterOption {
	return func(w *SegmentWriter) { w.segmentNum = n }
}

// NewSegmentWriter creates a new SegmentWriter that writes .rca files to outputDir.
func NewSegmentWriter(outputDir string, opts ...WriterOption) (*SegmentWriter, error) {
	w := &SegmentWriter{
		outputDir:   outputDir,
		segmentSize: DefaultSegmentSize,
		zstdLevel:   DefaultZstdLevel,
		segmentNum:  1,
	}
	for _, opt := range opts {
		opt(w)
	}

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating output dir: %w", err)
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevel(w.zstdLevel)))
	if err != nil {
		return nil, fmt.Errorf("creating zstd encoder: %w", err)
	}
	w.encoder = enc

	if err := w.openSegment(); err != nil {
		return nil, err
	}

	return w, nil
}

// SegmentNum returns the current segment number.
func (w *SegmentWriter) SegmentNum() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segmentNum
}

// TotalRepos returns the total number of repos written across all segments.
func (w *SegmentWriter) TotalRepos() uint32 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.totalRepos
}

// BytesWritten returns the total bytes written across all segments.
func (w *SegmentWriter) BytesWritten() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bytesWritten
}

func (w *SegmentWriter) segmentPath(num int) string {
	return filepath.Join(w.outputDir, fmt.Sprintf("segment_%04d.rca", num))
}

func (w *SegmentWriter) openSegment() error {
	path := w.segmentPath(w.segmentNum)
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating segment file %s: %w", path, err)
	}

	w.file = f
	w.offset = 0
	w.repoCount = 0
	w.indexEntries = w.indexEntries[:0]
	w.createdAt = time.Now()

	// Write file header (values finalized later).
	header := &FileHeader{
		Magic:     Magic,
		Version:   Version,
		CreatedAt: w.createdAt.UnixMicro(),
	}
	if err := writeFileHeader(f, header); err != nil {
		return fmt.Errorf("writing file header: %w", err)
	}
	w.offset = FileHeaderSize

	return nil
}

// WriteRepo serializes a CrawledRepo into the current segment.
// If the segment exceeds the target size, it finalizes and opens a new one.
func (w *SegmentWriter) WriteRepo(repo *CrawledRepo) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	blockData, blockCRC, err := w.serializeRepoBlock(repo)
	if err != nil {
		return fmt.Errorf("serializing repo %s: %w", repo.DID, err)
	}

	repoOffset := w.offset

	if _, err := w.file.Write(blockData); err != nil {
		return fmt.Errorf("writing repo block: %w", err)
	}

	w.offset += int64(len(blockData))
	w.repoCount++
	w.totalRepos++
	w.bytesWritten += int64(len(blockData))

	w.indexEntries = append(w.indexEntries, IndexEntry{
		DID:    repo.DID,
		Offset: repoOffset,
		Size:   uint32(len(blockData)),
		CRC32:  blockCRC,
	})

	// Check if we need to rotate segments.
	if w.offset >= w.segmentSize {
		if err := w.finalizeSegment(); err != nil {
			return fmt.Errorf("finalizing segment: %w", err)
		}
		w.segmentNum++
		if err := w.openSegment(); err != nil {
			return fmt.Errorf("opening new segment: %w", err)
		}
	}

	return nil
}

// serializeRepoBlock builds the binary repo block for a single CrawledRepo.
// Returns the full block bytes and its CRC32.
func (w *SegmentWriter) serializeRepoBlock(repo *CrawledRepo) ([]byte, uint32, error) {
	// Sort collection names for deterministic output.
	collNames := make([]string, 0, len(repo.Collections))
	for name := range repo.Collections {
		collNames = append(collNames, name)
	}
	sort.Strings(collNames)

	// Compress each collection independently.
	type collBlock struct {
		name       string
		records    []Record
		compressed []byte
	}
	collBlocks := make([]collBlock, 0, len(collNames))
	for _, name := range collNames {
		records := repo.Collections[name]
		if len(records) == 0 {
			continue
		}

		// Serialize the uncompressed collection data.
		var raw bytes.Buffer
		for _, rec := range records {
			if err := writeLenPrefixed(&raw, []byte(rec.RKey)); err != nil {
				return nil, 0, err
			}
			if err := writeLenPrefixed32(&raw, rec.JSON); err != nil {
				return nil, 0, err
			}
		}

		compressed := w.encoder.EncodeAll(raw.Bytes(), nil)
		collBlocks = append(collBlocks, collBlock{
			name:       name,
			records:    records,
			compressed: compressed,
		})
	}

	// Build the complete repo block into a buffer.
	// We write everything after total_block_size first, then prepend the size.
	var inner bytes.Buffer

	// DID, PDS, Rev
	if err := writeLenPrefixed(&inner, []byte(repo.DID)); err != nil {
		return nil, 0, err
	}
	if err := writeLenPrefixed(&inner, []byte(repo.PDS)); err != nil {
		return nil, 0, err
	}
	if err := writeLenPrefixed(&inner, []byte(repo.Rev)); err != nil {
		return nil, 0, err
	}

	// CrawledAt
	if err := writeInt64(&inner, repo.CrawledAt.UnixMicro()); err != nil {
		return nil, 0, err
	}

	// Collection count
	if err := writeUint16(&inner, uint16(len(collBlocks))); err != nil {
		return nil, 0, err
	}

	// CRC32 placeholder — we'll fill this after computing.
	crcOffset := inner.Len()
	if err := writeUint32(&inner, 0); err != nil {
		return nil, 0, err
	}

	// Collection TOC entries
	for _, cb := range collBlocks {
		if err := writeLenPrefixed(&inner, []byte(cb.name)); err != nil {
			return nil, 0, err
		}
		if err := writeUint32(&inner, uint32(len(cb.records))); err != nil {
			return nil, 0, err
		}
		if err := writeUint32(&inner, uint32(len(cb.compressed))); err != nil {
			return nil, 0, err
		}
	}

	// Collection data blocks (compressed)
	for _, cb := range collBlocks {
		if _, err := inner.Write(cb.compressed); err != nil {
			return nil, 0, err
		}
	}

	// Compute CRC32 of everything after the CRC32 field (TOC + data).
	innerBytes := inner.Bytes()
	afterCRC := innerBytes[crcOffset+4:]
	crcVal := checksumCRC32(afterCRC)

	// Patch in the CRC32 value.
	innerBytes[crcOffset] = byte(crcVal)
	innerBytes[crcOffset+1] = byte(crcVal >> 8)
	innerBytes[crcOffset+2] = byte(crcVal >> 16)
	innerBytes[crcOffset+3] = byte(crcVal >> 24)

	// Prepend total_block_size (4 bytes + inner).
	totalSize := uint32(4 + len(innerBytes))
	var block bytes.Buffer
	block.Grow(int(totalSize))
	if err := writeUint32(&block, totalSize); err != nil {
		return nil, 0, err
	}
	block.Write(innerBytes)

	blockBytes := block.Bytes()
	blockCRC := crc32.ChecksumIEEE(blockBytes)
	return blockBytes, blockCRC, nil
}

// finalizeSegment writes the index section and footer, then updates the file header.
func (w *SegmentWriter) finalizeSegment() error {
	indexOffset := w.offset

	// Sort index entries by DID for binary search.
	sort.Slice(w.indexEntries, func(i, j int) bool {
		return w.indexEntries[i].DID < w.indexEntries[j].DID
	})

	// Write index section.
	var indexBuf bytes.Buffer
	if err := writeUint32(&indexBuf, uint32(len(w.indexEntries))); err != nil {
		return err
	}
	for _, entry := range w.indexEntries {
		if err := writeLenPrefixed(&indexBuf, []byte(entry.DID)); err != nil {
			return err
		}
		if err := writeInt64(&indexBuf, entry.Offset); err != nil {
			return err
		}
		if err := writeUint32(&indexBuf, entry.Size); err != nil {
			return err
		}
		if err := writeUint32(&indexBuf, entry.CRC32); err != nil {
			return err
		}
	}

	indexData := indexBuf.Bytes()
	indexCRC := checksumCRC32(indexData)

	if _, err := w.file.Write(indexData); err != nil {
		return fmt.Errorf("writing index section: %w", err)
	}
	w.offset += int64(len(indexData))

	// Write footer.
	footer := &FileFooter{
		IndexOffset: indexOffset,
		IndexSize:   uint32(len(indexData)),
		RepoCount:   w.repoCount,
		CRC32:       indexCRC,
		Magic:       Magic,
	}
	if err := writeFileFooter(w.file, footer); err != nil {
		return fmt.Errorf("writing footer: %w", err)
	}

	// Update the file header with repo count and index offset.
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seeking to header: %w", err)
	}
	header := &FileHeader{
		Magic:       Magic,
		Version:     Version,
		CreatedAt:   w.createdAt.UnixMicro(),
		RepoCount:   w.repoCount,
		IndexOffset: indexOffset,
	}
	if err := writeFileHeader(w.file, header); err != nil {
		return fmt.Errorf("updating file header: %w", err)
	}

	return w.file.Close()
}

// Close finalizes the current segment and releases resources.
func (w *SegmentWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	if err := w.finalizeSegment(); err != nil {
		return err
	}

	w.encoder.Close()
	w.file = nil
	return nil
}

package repoarchive

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
)

// SegmentReader reads .rca segment files sequentially with optional collection filtering.
type SegmentReader struct {
	file      *os.File
	header    *FileHeader
	decoder   *zstd.Decoder
	filter    map[string]bool // nil means accept all collections
	offset    int64
	reposRead uint32

	currentRepo *repoIterator
}

// repoIterator holds state for iterating through one repo block.
type repoIterator struct {
	DID             string
	PDS             string
	Rev             string
	CrawledAt       time.Time
	CollectionCount uint16

	toc         []CollectionTOCEntry
	tocIndex    int
	dataOffset  int64  // current offset within collDataBuf
	collDataBuf []byte // raw compressed collection data blocks
	reader      *SegmentReader

	currentColl *collectionIterator
}

// collectionIterator holds state for iterating through one collection's records.
type collectionIterator struct {
	Name        string
	RecordCount uint32
	data        []byte
	dataReader  *bytes.Reader
	recordsRead uint32
}

// OpenSegment opens an .rca segment file for reading.
func OpenSegment(path string) (*SegmentReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening segment: %w", err)
	}

	header, err := readFileHeader(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("reading header: %w", err)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("creating zstd decoder: %w", err)
	}

	return &SegmentReader{
		file:    f,
		header:  header,
		decoder: decoder,
		offset:  FileHeaderSize,
	}, nil
}

// Header returns the file header.
func (r *SegmentReader) Header() *FileHeader {
	return r.header
}

// SetCollectionFilter configures which collections to decompress during iteration.
// Only the named collections will be decompressed; others are skipped by advancing
// past their compressed bytes. Calling with no arguments clears the filter.
func (r *SegmentReader) SetCollectionFilter(collections ...string) {
	if len(collections) == 0 {
		r.filter = nil
		return
	}
	r.filter = make(map[string]bool, len(collections))
	for _, c := range collections {
		r.filter[c] = true
	}
}

// Next advances to the next repo block. Returns false at EOF or on error.
func (r *SegmentReader) Next() bool {
	if r.header.RepoCount > 0 && r.reposRead >= r.header.RepoCount {
		return false
	}
	if r.header.IndexOffset > 0 && r.offset >= r.header.IndexOffset {
		return false
	}

	if _, err := r.file.Seek(r.offset, io.SeekStart); err != nil {
		return false
	}

	totalBlockSize, err := readUint32(r.file)
	if err != nil {
		return false
	}

	blockData := make([]byte, totalBlockSize-4)
	if _, err := io.ReadFull(r.file, blockData); err != nil {
		return false
	}
	br := bytes.NewReader(blockData)

	did, err := readLenPrefixed(br)
	if err != nil {
		return false
	}
	pds, err := readLenPrefixed(br)
	if err != nil {
		return false
	}
	rev, err := readLenPrefixed(br)
	if err != nil {
		return false
	}
	crawledAtUS, err := readInt64(br)
	if err != nil {
		return false
	}
	collCount, err := readUint16(br)
	if err != nil {
		return false
	}
	// Skip CRC32 field.
	if _, err := readUint32(br); err != nil {
		return false
	}

	toc := make([]CollectionTOCEntry, collCount)
	for i := range toc {
		name, err := readLenPrefixed(br)
		if err != nil {
			return false
		}
		recCount, err := readUint32(br)
		if err != nil {
			return false
		}
		compSize, err := readUint32(br)
		if err != nil {
			return false
		}
		toc[i] = CollectionTOCEntry{
			Name:           string(name),
			RecordCount:    recCount,
			CompressedSize: compSize,
		}
	}

	// Remaining bytes in blockData are the compressed collection data blocks.
	dataStartInBlock := len(blockData) - br.Len()

	r.currentRepo = &repoIterator{
		DID:             string(did),
		PDS:             string(pds),
		Rev:             string(rev),
		CrawledAt:       time.UnixMicro(crawledAtUS),
		CollectionCount: collCount,
		toc:             toc,
		collDataBuf:     blockData[dataStartInBlock:],
		reader:          r,
	}

	r.offset += int64(totalBlockSize)
	r.reposRead++
	return true
}

// Repo returns the current repo iterator. Only valid after Next() returns true.
func (r *SegmentReader) Repo() *repoIterator {
	return r.currentRepo
}

// ReposRead returns the count of repos read so far.
func (r *SegmentReader) ReposRead() uint32 {
	return r.reposRead
}

// Close closes the segment reader and releases resources.
func (r *SegmentReader) Close() error {
	r.decoder.Close()
	return r.file.Close()
}

// NextCollection advances to the next collection in the current repo.
// Collections not matching the filter are skipped without decompression.
func (ri *repoIterator) NextCollection() bool {
	for ri.tocIndex < len(ri.toc) {
		entry := ri.toc[ri.tocIndex]
		ri.tocIndex++

		if ri.reader.filter != nil && !ri.reader.filter[entry.Name] {
			ri.dataOffset += int64(entry.CompressedSize)
			continue
		}

		start := int(ri.dataOffset)
		end := start + int(entry.CompressedSize)
		if end > len(ri.collDataBuf) {
			return false
		}
		compressed := ri.collDataBuf[start:end]
		ri.dataOffset += int64(entry.CompressedSize)

		decompressed, err := ri.reader.decoder.DecodeAll(compressed, nil)
		if err != nil {
			return false
		}

		ri.currentColl = &collectionIterator{
			Name:        entry.Name,
			RecordCount: entry.RecordCount,
			data:        decompressed,
			dataReader:  bytes.NewReader(decompressed),
		}
		return true
	}
	return false
}

// Collection returns the current collection iterator.
func (ri *repoIterator) Collection() *collectionIterator {
	return ri.currentColl
}

// TOC returns the collection table-of-contents entries for this repo,
// including record counts, without requiring decompression.
func (ri *repoIterator) TOC() []CollectionTOCEntry {
	return ri.toc
}

// NextRecord advances to the next record in the current collection.
func (ci *collectionIterator) NextRecord() bool {
	if ci.recordsRead >= ci.RecordCount {
		return false
	}
	ci.recordsRead++
	return true
}

// Record reads and returns the current record. Call after NextRecord() returns true.
func (ci *collectionIterator) Record() (*Record, error) {
	rkey, err := readLenPrefixed(ci.dataReader)
	if err != nil {
		return nil, fmt.Errorf("reading rkey: %w", err)
	}
	jsonData, err := readLenPrefixed32(ci.dataReader)
	if err != nil {
		return nil, fmt.Errorf("reading json: %w", err)
	}
	return &Record{
		RKey: string(rkey),
		JSON: jsonData,
	}, nil
}

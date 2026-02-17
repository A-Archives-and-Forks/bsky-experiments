package repoarchive

import "time"

// File format magic bytes and version.
var Magic = [8]byte{'R', 'E', 'P', 'O', 'A', 'R', 'C', 'H'}

const (
	Version = uint16(1)

	FileHeaderSize = 64
	FileFooterSize = 32

	// DefaultSegmentSize is the target segment file size before starting a new one.
	DefaultSegmentSize = 2 * 1024 * 1024 * 1024 // 2GB

	// DefaultZstdLevel is the default zstd compression level.
	DefaultZstdLevel = 3
)

// FileHeader is the 64-byte header at the start of each .rca segment file.
type FileHeader struct {
	Magic       [8]byte  // "REPOARCH"
	Version     uint16   // 1
	Flags       uint16   // reserved
	CreatedAt   int64    // unix microseconds
	RepoCount   uint32   // set at finalize
	IndexOffset int64    // set at finalize
	Reserved    [32]byte // zero-filled, pads header to exactly 64 bytes
}

// FileFooter is the 32-byte footer at the end of each .rca segment file.
type FileFooter struct {
	IndexOffset int64    // byte offset of index section
	IndexSize   uint32   // byte size of index section
	RepoCount   uint32   // total repos in this segment
	CRC32       uint32   // CRC32 of the index section
	Reserved    [4]byte  // pads footer to exactly 32 bytes
	Magic       [8]byte  // "REPOARCH"
}

// IndexEntry is one entry in the per-segment DID index.
type IndexEntry struct {
	DID    string
	Offset int64  // byte offset of the repo block in the segment
	Size   uint32 // byte size of the repo block
	CRC32  uint32 // CRC32 of the repo block
}

// RepoHeader contains metadata for a single repo block.
type RepoHeader struct {
	TotalBlockSize  uint32 // enables skipping entire repo
	DID             string
	PDS             string
	Rev             string // repo revision for future incremental
	CrawledAt       time.Time
	CollectionCount uint16
	CRC32           uint32 // CRC32 of everything after the header
}

// CollectionTOCEntry describes one collection within a repo block.
type CollectionTOCEntry struct {
	Name           string
	RecordCount    uint32
	CompressedSize uint32 // byte size of the zstd-compressed data block
}

// Record is a single record within a collection data block.
type Record struct {
	RKey string
	JSON []byte // CID-stripped JSON
}

// SerializedRepo is a pre-compressed repo block ready for sequential file writing.
// Workers produce these by calling SerializeRepoBlock, and the writer consumes them
// via WriteSerializedRepo — keeping compression parallel and I/O sequential.
type SerializedRepo struct {
	DID       string
	BlockData []byte
	BlockCRC  uint32
}

// CrawledRepo is the in-memory representation of a crawled repo ready for writing.
type CrawledRepo struct {
	DID         string
	PDS         string
	Rev         string
	CrawledAt   time.Time
	Collections map[string][]Record // collection name -> records
}

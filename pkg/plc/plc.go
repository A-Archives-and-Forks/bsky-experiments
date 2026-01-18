package plc

import (
	"time"

	"github.com/goccy/go-json"
)

// PLCOperation represents a single operation from the PLC export
type PLCOperation struct {
	DID       string          `json:"did"`
	CID       string          `json:"cid"`
	CreatedAt time.Time       `json:"createdAt"`
	Nullified bool            `json:"nullified"`
	Operation json.RawMessage `json:"operation"`
}

// PLCOperationPayload represents the operation field structure
// This handles both legacy "create" operations and newer "plc_operation" format
type PLCOperationPayload struct {
	Type string `json:"type"` // "create", "plc_operation", "plc_tombstone"

	// For plc_operation and create types
	AlsoKnownAs     []string          `json:"alsoKnownAs"`     // Handles in at:// format
	Services        map[string]Service `json:"services"`        // Service endpoints
	VerificationMethods map[string]string `json:"verificationMethods"`
	RotationKeys    []string          `json:"rotationKeys"`

	// Legacy format fields
	Handle          string `json:"handle"`          // Legacy: bare handle
	Service         string `json:"service"`         // Legacy: PDS URL
	RecoveryKey     string `json:"recoveryKey"`
	SigningKey      string `json:"signingKey"`

	Prev string `json:"prev"` // Previous operation CID (null for genesis)
	Sig  string `json:"sig"`  // Signature
}

// Service represents a service endpoint in the operation
type Service struct {
	Type     string `json:"type"`
	Endpoint string `json:"endpoint"`
}

// PLCBatch represents a batch of PLC operations for ClickHouse insertion
type PLCBatch struct {
	DID           string
	CID           string
	OperationType string
	CreatedAt     time.Time
	Handle        string
	PDS           string
	OperationJSON string
	TimeUS        int64
}

// ExtractHandleAndPDS extracts the handle and PDS endpoint from a PLC operation
func ExtractHandleAndPDS(op *PLCOperation) (handle string, pds string) {
	var payload PLCOperationPayload
	if err := json.Unmarshal(op.Operation, &payload); err != nil {
		return "", ""
	}

	// Try new format first (alsoKnownAs for handle)
	if len(payload.AlsoKnownAs) > 0 {
		// Handle is usually in at:// format, extract the bare handle
		handle = extractHandleFromAKA(payload.AlsoKnownAs[0])
	} else if payload.Handle != "" {
		// Legacy format
		handle = payload.Handle
	}

	// Try new format for PDS (services.atproto_pds.endpoint)
	if service, ok := payload.Services["atproto_pds"]; ok {
		pds = service.Endpoint
	} else if payload.Service != "" {
		// Legacy format
		pds = payload.Service
	}

	return handle, pds
}

// extractHandleFromAKA extracts bare handle from at:// URI
// e.g., "at://user.bsky.social" -> "user.bsky.social"
func extractHandleFromAKA(aka string) string {
	const prefix = "at://"
	if len(aka) > len(prefix) && aka[:len(prefix)] == prefix {
		return aka[len(prefix):]
	}
	return aka
}

// GetOperationType extracts the operation type from the payload
func GetOperationType(op *PLCOperation) string {
	var payload struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(op.Operation, &payload); err != nil {
		return "unknown"
	}
	if payload.Type == "" {
		return "unknown"
	}
	return payload.Type
}

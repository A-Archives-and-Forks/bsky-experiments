package crawler

import (
	"fmt"

	"github.com/goccy/go-json"
	"github.com/ipfs/go-cid"
	cbornode "github.com/ipfs/go-ipld-cbor"
)

// cborToStrippedJSON decodes a DAG-CBOR block into JSON, converting CID link
// objects to {"$link": "..."} and stripping redundant "cid" fields (StrongRef)
// in a single pass.
func cborToStrippedJSON(raw []byte) ([]byte, error) {
	var obj interface{}
	if err := cbornode.DecodeInto(raw, &obj); err != nil {
		return nil, fmt.Errorf("cbor decode: %w", err)
	}
	stripped := convertAndStripCIDs(obj)
	return json.Marshal(stripped)
}

// convertAndStripCIDs walks an interface{} tree from CBOR decode:
//   - Converts cid.Cid values to map{"$link": cid.String()}
//   - Removes "cid" keys from maps (StrongRef deduplication)
func convertAndStripCIDs(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		for k, v := range val {
			if k == "cid" {
				delete(val, k)
				continue
			}
			val[k] = convertAndStripCIDs(v)
		}
		return val
	case map[interface{}]interface{}:
		// CBOR may decode map keys as interface{}.
		result := make(map[string]interface{}, len(val))
		for k, v := range val {
			key := fmt.Sprint(k)
			if key == "cid" {
				continue
			}
			result[key] = convertAndStripCIDs(v)
		}
		return result
	case []interface{}:
		for i, v := range val {
			val[i] = convertAndStripCIDs(v)
		}
		return val
	case cid.Cid:
		return map[string]interface{}{"$link": val.String()}
	default:
		return v
	}
}

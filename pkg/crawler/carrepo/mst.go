package carrepo

import (
	"bytes"
	"fmt"

	"github.com/bluesky-social/indigo/mst"
	"github.com/ipfs/go-cid"
)

// walkLeaves walks all MST leaf entries, calling cb with each record path and
// record CID. Unlike indigo's WalkLeavesFrom, this skips layer detection
// (no SHA256 hashing), MerkleSearchTree object allocation, and nodeEntry
// intermediaries — directly walking decoded NodeData.
func walkLeaves(blocks map[cid.Cid][]byte, root cid.Cid, cb func(key string, val cid.Cid) error) error {
	return walkNode(blocks, root, cb)
}

func walkNode(blocks map[cid.Cid][]byte, nodeCid cid.Cid, cb func(key string, val cid.Cid) error) error {
	raw, ok := blocks[nodeCid]
	if !ok {
		return fmt.Errorf("missing MST node block: %s", nodeCid)
	}

	var nd mst.NodeData
	if err := nd.UnmarshalCBOR(bytes.NewReader(raw)); err != nil {
		return fmt.Errorf("decoding MST node: %w", err)
	}

	// Recurse into left subtree first (smaller keys).
	if nd.Left != nil {
		if err := walkNode(blocks, *nd.Left, cb); err != nil {
			return err
		}
	}

	// Walk entries, reconstructing keys via prefix compression.
	var lastKey []byte
	for i := range nd.Entries {
		e := &nd.Entries[i]

		// Reconstruct full key: keep first PrefixLen bytes of lastKey, append suffix.
		lastKey = append(lastKey[:e.PrefixLen], e.KeySuffix...)

		if err := cb(string(lastKey), e.Val); err != nil {
			return err
		}

		// Recurse into right subtree (keys between this entry and the next).
		if e.Tree != nil {
			if err := walkNode(blocks, *e.Tree, cb); err != nil {
				return err
			}
		}
	}

	return nil
}

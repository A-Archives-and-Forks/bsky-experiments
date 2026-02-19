package crawler

import (
	"encoding/hex"
	"encoding/json"
	"testing"
)

func TestCborToStrippedJSON(t *testing.T) {
	tests := []struct {
		name    string
		cborHex string // hex-encoded CBOR input
		want    string // expected JSON output
	}{
		{
			name:    "unsigned integer 0",
			cborHex: "00",
			want:    "0",
		},
		{
			name:    "unsigned integer 23",
			cborHex: "17",
			want:    "23",
		},
		{
			name:    "unsigned integer 24",
			cborHex: "1818",
			want:    "24",
		},
		{
			name:    "unsigned integer 1000",
			cborHex: "1903e8",
			want:    "1000",
		},
		{
			name:    "negative integer -1",
			cborHex: "20",
			want:    "-1",
		},
		{
			name:    "negative integer -100",
			cborHex: "3863",
			want:    "-100",
		},
		{
			name:    "empty string",
			cborHex: "60",
			want:    `""`,
		},
		{
			name:    "string hello",
			cborHex: "6568656c6c6f",
			want:    `"hello"`,
		},
		{
			name:    "string with quotes",
			cborHex: "63612262", // a"b
			want:    `"a\"b"`,
		},
		{
			name:    "string with backslash",
			cborHex: "63615c62", // a\b
			want:    `"a\\b"`,
		},
		{
			name:    "string with newline",
			cborHex: "63610a62", // a\nb
			want:    `"a\nb"`,
		},
		{
			name:    "true",
			cborHex: "f5",
			want:    "true",
		},
		{
			name:    "false",
			cborHex: "f4",
			want:    "false",
		},
		{
			name:    "null",
			cborHex: "f6",
			want:    "null",
		},
		{
			name:    "empty array",
			cborHex: "80",
			want:    "[]",
		},
		{
			name:    "array [1, 2, 3]",
			cborHex: "83010203",
			want:    "[1,2,3]",
		},
		{
			name:    "nested array [1, [2, 3]]",
			cborHex: "8201820203",
			want:    "[1,[2,3]]",
		},
		{
			name:    "empty map",
			cborHex: "a0",
			want:    "{}",
		},
		{
			name:    "map {a: 1}",
			cborHex: "a1616101",
			want:    `{"a":1}`,
		},
		{
			name:    "map strips cid key",
			cborHex: "a2636369646568656c6c6f6375726966666f6f626172", // {"cid":"hello","uri":"foobar"}
			want:    `{"uri":"foobar"}`,
		},
		{
			name:    "float32 100000.0",
			cborHex: "fa47c35000",
			want:    "100000",
		},
		{
			name:    "float64 1.1",
			cborHex: "fb3ff199999999999a",
			want:    "1.1",
		},
		{
			name:    "byte string becomes null",
			cborHex: "43010203", // 3-byte byte string
			want:    "null",
		},
		{
			name:    "float64 NaN becomes null",
			cborHex: "fb7ff8000000000000", // float64 NaN
			want:    "null",
		},
		{
			name:    "float64 +Inf becomes null",
			cborHex: "fb7ff0000000000000", // float64 +Infinity
			want:    "null",
		},
		{
			name:    "float64 -Inf becomes null",
			cborHex: "fbfff0000000000000", // float64 -Infinity
			want:    "null",
		},
		{
			name:    "float16 NaN becomes null",
			cborHex: "f97e00", // float16 NaN
			want:    "null",
		},
		{
			name:    "float16 +Inf becomes null",
			cborHex: "f97c00", // float16 +Infinity
			want:    "null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := hex.DecodeString(tt.cborHex)
			if err != nil {
				t.Fatalf("bad test hex: %v", err)
			}

			got, err := cborToStrippedJSON(raw)
			if err != nil {
				t.Fatalf("cborToStrippedJSON error: %v", err)
			}

			if string(got) != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}

			// Verify output is valid JSON.
			if !json.Valid(got) {
				t.Errorf("output is not valid JSON: %s", got)
			}
		})
	}
}

func TestCborToStrippedJSON_CIDLink(t *testing.T) {
	// Build a CBOR map with a CID tag 42 value:
	// {"img": tag(42, bytes(0x00 + CIDv1 raw-identity))}
	//
	// Use a CIDv1 with raw codec (0x55) and identity multihash (0x00, 0x01, 0x42):
	// Multicodec prefix 0x00 + CID bytes: 0x01 (version) + 0x55 (raw) + 0x00 0x01 0x42 (identity hash)
	//
	// CBOR: a1                   -- map(1)
	//       63 696d67            -- text(3) "img"
	//       d8 2a                -- tag(42)
	//       46                   -- bytes(6)
	//       00 01 55 00 01 42    -- 0x00 prefix + CIDv1(raw, identity(0x42))
	cborHex := "a163696d67d82a460001550001fe"
	raw, err := hex.DecodeString(cborHex)
	if err != nil {
		t.Fatalf("bad hex: %v", err)
	}

	got, err := cborToStrippedJSON(raw)
	if err != nil {
		t.Fatalf("cborToStrippedJSON error: %v", err)
	}

	// Verify the output contains a $link key.
	var parsed map[string]any
	if err := json.Unmarshal(got, &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v (raw: %s)", err, got)
	}

	imgVal, ok := parsed["img"].(map[string]any)
	if !ok {
		t.Fatalf("expected img to be a map, got %T: %v", parsed["img"], parsed["img"])
	}
	if _, ok := imgVal["$link"]; !ok {
		t.Errorf("expected $link key in CID object, got %v", imgVal)
	}
}

func BenchmarkCborToStrippedJSON(b *testing.B) {
	// Build a realistic-ish CBOR payload: a map with several string fields and a nested array.
	// {"text":"hello world","createdAt":"2024-01-01T00:00:00Z","langs":["en"],"reply":{"uri":"at://foo","cid":"bafyxyz"}}
	//
	// Hand-built CBOR hex for this structure:
	// a4                          -- map(4)
	// 64 74657874                 -- text(4) "text"
	// 6b 68656c6c6f20776f726c64   -- text(11) "hello world"
	// 69 637265617465644174       -- text(9) "createdAt"
	// 74 323032342d30312d30315430303a30303a30305a  -- text(20) "2024-01-01T00:00:00Z"
	// 65 6c616e6773               -- text(5) "langs"
	// 81                          -- array(1)
	// 62 656e                     -- text(2) "en"
	// 65 7265706c79               -- text(5) "reply"
	// a2                          -- map(2)
	// 63 757269                   -- text(3) "uri"
	// 68 61743a2f2f666f6f         -- text(8) "at://foo"
	// 63 636964                   -- text(3) "cid"
	// 67 62616679787970           -- text(7) "bafyxyp"
	cborHex := "a4" +
		"6474657874" + "6b68656c6c6f20776f726c64" +
		"69637265617465644174" + "74323032342d30312d30315430303a30303a30305a" +
		"656c616e6773" + "81" + "62656e" +
		"657265706c79" + "a2" + "63757269" + "6861743a2f2f666f6f" + "63636964" + "6762616679787970"
	raw, err := hex.DecodeString(cborHex)
	if err != nil {
		b.Fatalf("bad hex: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		result, err := cborToStrippedJSON(raw)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

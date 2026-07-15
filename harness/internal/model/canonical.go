package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// JSON is an immutable, canonical JSON value. Objects have sorted keys,
// insignificant whitespace is removed, duplicate keys are rejected, and only
// integer numbers are admitted by the R5 model.
type JSON struct {
	raw string
}

func NewJSON(raw []byte) (JSON, error) {
	canonical, err := CanonicalizeJSON(raw)
	if err != nil {
		return JSON{}, err
	}
	if len(canonical) > MaxCanonicalJSONBytes {
		return JSON{}, limit("json", len(canonical), MaxCanonicalJSONBytes)
	}
	return JSON{raw: string(canonical)}, nil
}

func JSONFrom(value any) (JSON, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return JSON{}, fmt.Errorf("marshal JSON: %w", err)
	}
	return NewJSON(raw)
}

func (j JSON) Bytes() []byte  { return append([]byte(nil), j.raw...) }
func (j JSON) String() string { return j.raw }
func (j JSON) IsZero() bool   { return j.raw == "" }

func (j JSON) MarshalJSON() ([]byte, error) {
	if j.IsZero() {
		return nil, invalid("json", "zero canonical JSON")
	}
	return j.Bytes(), nil
}

func CanonicalMarshal(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical JSON: %w", err)
	}
	return CanonicalizeJSON(raw)
}

func CanonicalizeJSON(raw []byte) ([]byte, error) {
	if !utf8.Valid(raw) {
		return nil, invalid("json", "must be valid UTF-8 before decoding")
	}
	if err := validateUnicodeEscapes(raw); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := decodeCanonicalValue(decoder)
	if err != nil {
		return nil, fmt.Errorf("canonical JSON: %w", err)
	}
	if decoder.More() {
		return nil, invalid("json", "unexpected trailing value")
	}
	if token, err := decoder.Token(); err == nil || token != nil {
		return nil, invalid("json", "unexpected trailing value")
	}
	canonical, err := appendCanonical(nil, value)
	if err != nil {
		return nil, err
	}
	return canonical, nil
}

func validateUnicodeEscapes(raw []byte) error {
	inString := false
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '"':
			inString = !inString
		case '\\':
			if !inString {
				continue
			}
			i++
			if i >= len(raw) {
				return invalid("json", "unterminated string escape")
			}
			if raw[i] != 'u' {
				continue
			}
			code, ok := decodeHex4(raw, i+1)
			if !ok {
				return invalid("json", "invalid Unicode escape")
			}
			i += 4
			if code >= 0xdc00 && code <= 0xdfff {
				return invalid("json", "unpaired low surrogate")
			}
			if code < 0xd800 || code > 0xdbff {
				continue
			}
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return invalid("json", "unpaired high surrogate")
			}
			low, ok := decodeHex4(raw, i+3)
			if !ok || low < 0xdc00 || low > 0xdfff {
				return invalid("json", "invalid surrogate pair")
			}
			i += 6
		}
	}
	return nil
}

func decodeHex4(raw []byte, start int) (uint16, bool) {
	if start+4 > len(raw) {
		return 0, false
	}
	var result uint16
	for _, value := range raw[start : start+4] {
		result <<= 4
		switch {
		case value >= '0' && value <= '9':
			result += uint16(value - '0')
		case value >= 'a' && value <= 'f':
			result += uint16(value-'a') + 10
		case value >= 'A' && value <= 'F':
			result += uint16(value-'A') + 10
		default:
			return 0, false
		}
	}
	return result, true
}

func decodeCanonicalValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			object := make(map[string]any)
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, invalid("json", "object key is not a string")
				}
				if _, exists := object[key]; exists {
					return nil, invalid("json", "duplicate object key")
				}
				child, err := decodeCanonicalValue(decoder)
				if err != nil {
					return nil, err
				}
				object[key] = child
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
				return nil, invalid("json", "unterminated object")
			}
			return object, nil
		case '[':
			var array []any
			for decoder.More() {
				child, err := decodeCanonicalValue(decoder)
				if err != nil {
					return nil, err
				}
				array = append(array, child)
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
				return nil, invalid("json", "unterminated array")
			}
			return array, nil
		default:
			return nil, invalid("json", "unexpected delimiter")
		}
	case json.Number:
		text := value.String()
		if strings.ContainsAny(text, ".eE") {
			return nil, invalid("json number", "R5 canonical JSON permits integers only")
		}
		if text == "-0" {
			return json.Number("0"), nil
		}
		if _, err := strconv.ParseInt(text, 10, 64); err != nil {
			if _, unsignedErr := strconv.ParseUint(text, 10, 64); unsignedErr != nil {
				return nil, invalid("json number", "integer is outside 64-bit range")
			}
		}
		return value, nil
	case string, bool, nil:
		return value, nil
	default:
		return nil, invalid("json", "unsupported value")
	}
}

func appendCanonical(dst []byte, value any) ([]byte, error) {
	switch value := value.(type) {
	case nil:
		return append(dst, "null"...), nil
	case bool:
		return strconv.AppendBool(dst, value), nil
	case string:
		return appendJSONString(dst, value), nil
	case json.Number:
		return append(dst, value.String()...), nil
	case []any:
		dst = append(dst, '[')
		for i, child := range value {
			if i > 0 {
				dst = append(dst, ',')
			}
			var err error
			dst, err = appendCanonical(dst, child)
			if err != nil {
				return nil, err
			}
		}
		return append(dst, ']'), nil
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		dst = append(dst, '{')
		for i, key := range keys {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendJSONString(dst, key)
			dst = append(dst, ':')
			var err error
			dst, err = appendCanonical(dst, value[key])
			if err != nil {
				return nil, err
			}
		}
		return append(dst, '}'), nil
	default:
		return nil, invalid("json", "unsupported canonical value")
	}
}

func appendJSONString(dst []byte, value string) []byte {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
	encoded := buffer.Bytes()
	return append(dst, encoded[:len(encoded)-1]...)
}

type Digest struct {
	sum [sha256.Size]byte
}

func Sum(data []byte) Digest { return Digest{sum: sha256.Sum256(data)} }

func DigestFromBytes(value []byte) (Digest, error) {
	if len(value) != sha256.Size {
		return Digest{}, invalid("digest", "must contain exactly 32 bytes")
	}
	var digest Digest
	copy(digest.sum[:], value)
	return digest, nil
}

func ParseDigest(value string) (Digest, error) {
	if !strings.HasPrefix(value, "sha256:") {
		return Digest{}, invalid("digest", "must use sha256:<lowercase-hex>")
	}
	hexValue := strings.TrimPrefix(value, "sha256:")
	if len(hexValue) != sha256.Size*2 || strings.ToLower(hexValue) != hexValue {
		return Digest{}, invalid("digest", "must use 64 lowercase hex digits")
	}
	raw, err := hex.DecodeString(hexValue)
	if err != nil {
		return Digest{}, invalid("digest", "invalid hexadecimal digest")
	}
	return DigestFromBytes(raw)
}

func (d Digest) Bytes() []byte  { return append([]byte(nil), d.sum[:]...) }
func (d Digest) String() string { return "sha256:" + hex.EncodeToString(d.sum[:]) }
func (d Digest) IsZero() bool   { return d == Digest{} }

func (d Digest) MarshalJSON() ([]byte, error) {
	if d.IsZero() {
		return nil, invalid("digest", "zero digest")
	}
	return json.Marshal(d.String())
}

func canonicalTime(value time.Time) (time.Time, error) {
	if value.IsZero() {
		return time.Time{}, invalid("time", "must not be zero")
	}
	canonical := value.Round(0).UTC()
	wire := canonical.Format(time.RFC3339Nano)
	parsed, err := time.Parse(time.RFC3339Nano, wire)
	if err != nil || !parsed.Equal(canonical) || !time.Unix(0, canonical.UnixNano()).UTC().Equal(canonical) {
		return time.Time{}, invalid("time", "must round-trip as RFC3339Nano and Unix nanoseconds")
	}
	return canonical, nil
}

func formatTime(value time.Time) string {
	return value.Round(0).UTC().Format(time.RFC3339Nano)
}

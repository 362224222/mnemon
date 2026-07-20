package pagination

import (
	"encoding/base64"
	"encoding/json"
	"errors"
)

var ErrInvalidCursor = errors.New("invalid cursor")

type Cursor struct {
	Offset int `json:"offset"`
}

func EncodeCursor(cursor Cursor, signingKey []byte) (string, error) {
	if cursor.Offset < 0 || len(signingKey) == 0 {
		return "", ErrInvalidCursor
	}
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func DecodeCursor(token string, signingKey []byte) (Cursor, error) {
	if token == "" || len(signingKey) == 0 {
		return Cursor{}, ErrInvalidCursor
	}
	payload, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	var cursor Cursor
	if err := json.Unmarshal(payload, &cursor); err != nil || cursor.Offset < 0 {
		return Cursor{}, ErrInvalidCursor
	}
	return cursor, nil
}

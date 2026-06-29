package session

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	SchemaVersion = 1
	DefaultRelDir = ".mnemon/harness/sessions"
)

type Clock func() time.Time

type Record struct {
	SchemaVersion             int          `json:"schema_version"`
	ID                        string       `json:"id"`
	Title                     string       `json:"title,omitempty"`
	PrimaryActivationCarrier  string       `json:"primary_activation_carrier,omitempty"`
	DuplicateActivationPolicy string       `json:"duplicate_activation_policy,omitempty"`
	Attachments               []Attachment `json:"attachments,omitempty"`
	CreatedAt                 time.Time    `json:"created_at"`
	UpdatedAt                 time.Time    `json:"updated_at"`
}

type Attachment struct {
	Surface     string    `json:"surface"`
	ExternalRef string    `json:"external_ref"`
	Primary     bool      `json:"primary,omitempty"`
	AttachedAt  time.Time `json:"attached_at"`
}

type FileStore struct {
	dir string
	now Clock
}

func DefaultDir(root, explicit string) string {
	if strings.TrimSpace(explicit) != "" {
		return filepath.Clean(explicit)
	}
	root = strings.TrimSpace(root)
	if root == "" {
		root = "."
	}
	return filepath.Join(root, DefaultRelDir)
}

func NewFileStore(dir string, now Clock) *FileStore {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &FileStore{dir: strings.TrimSpace(dir), now: now}
}

func (s *FileStore) Start(record Record) (Record, error) {
	if err := validateID(record.ID); err != nil {
		return Record{}, err
	}
	if strings.TrimSpace(s.dir) == "" {
		return Record{}, fmt.Errorf("session store directory is required")
	}
	now := s.now()
	record.SchemaVersion = SchemaVersion
	record.ID = strings.TrimSpace(record.ID)
	record.Title = strings.TrimSpace(record.Title)
	record.PrimaryActivationCarrier = strings.TrimSpace(record.PrimaryActivationCarrier)
	record.DuplicateActivationPolicy = strings.TrimSpace(record.DuplicateActivationPolicy)
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.UpdatedAt = now
	if err := s.write(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func (s *FileStore) Load(id string) (Record, error) {
	if err := validateID(id); err != nil {
		return Record{}, err
	}
	data, err := os.ReadFile(s.path(id))
	if err != nil {
		return Record{}, err
	}
	var record Record
	if err := json.Unmarshal(data, &record); err != nil {
		return Record{}, fmt.Errorf("parse session record: %w", err)
	}
	if record.SchemaVersion != SchemaVersion {
		return Record{}, fmt.Errorf("session schema_version %d unsupported (want %d)", record.SchemaVersion, SchemaVersion)
	}
	if err := validateID(record.ID); err != nil {
		return Record{}, err
	}
	return record, nil
}

func (s *FileStore) Attach(id string, attachment Attachment) (Record, error) {
	record, err := s.Load(id)
	if err != nil {
		return Record{}, err
	}
	attachment.Surface = strings.TrimSpace(attachment.Surface)
	attachment.ExternalRef = strings.TrimSpace(attachment.ExternalRef)
	if attachment.Surface == "" {
		return Record{}, fmt.Errorf("session attachment surface is required")
	}
	if attachment.ExternalRef == "" {
		return Record{}, fmt.Errorf("session attachment external ref is required")
	}
	now := s.now()
	if attachment.AttachedAt.IsZero() {
		attachment.AttachedAt = now
	}
	updated := false
	for i := range record.Attachments {
		if record.Attachments[i].Surface == attachment.Surface && record.Attachments[i].ExternalRef == attachment.ExternalRef {
			record.Attachments[i] = attachment
			updated = true
			break
		}
	}
	if !updated {
		record.Attachments = append(record.Attachments, attachment)
	}
	if attachment.Primary {
		record.PrimaryActivationCarrier = attachment.Surface
	}
	record.UpdatedAt = now
	if err := s.write(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func (s *FileStore) write(record Record) error {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	path := s.path(record.ID)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s *FileStore) path(id string) string {
	return filepath.Join(s.dir, strings.TrimSpace(id)+".json")
}

func validateID(id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("session id is required")
	}
	if strings.ContainsAny(id, `/\`) {
		return fmt.Errorf("session id %q must not contain path separators", id)
	}
	return nil
}

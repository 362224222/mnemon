package model

import (
	"encoding/json"
	"time"
)

// Category represents the type of an insight.
type Category string

const (
	CategoryPreference Category = "preference"
	CategoryDecision   Category = "decision"
	CategoryFact       Category = "fact"
	CategoryInsight    Category = "insight"
	CategoryContext    Category = "context"
	CategoryGeneral    Category = "general"
)

var ValidCategories = map[Category]bool{
	CategoryPreference: true,
	CategoryDecision:   true,
	CategoryFact:       true,
	CategoryInsight:    true,
	CategoryContext:    true,
	CategoryGeneral:    true,
}

// Insight represents a memory node in the knowledge graph.
type Insight struct {
	ID          string     `json:"id"`
	Content     string     `json:"content"`
	Category    Category   `json:"category"`
	Importance  int        `json:"importance"`
	Tags        []string   `json:"tags"`
	Entities    []string   `json:"entities"`
	Source      string     `json:"source"`
	AccessCount int        `json:"access_count"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	DeletedAt   *time.Time `json:"deleted_at,omitempty"`
}

// TagsJSON returns tags as a JSON string for storage.
func (i *Insight) TagsJSON() string {
	b, _ := json.Marshal(i.Tags)
	return string(b)
}

// EntitiesJSON returns entities as a JSON string for storage.
func (i *Insight) EntitiesJSON() string {
	b, _ := json.Marshal(i.Entities)
	return string(b)
}

// ParseTags parses a JSON string into the Tags field.
func (i *Insight) ParseTags(s string) {
	_ = json.Unmarshal([]byte(s), &i.Tags)
	if i.Tags == nil {
		i.Tags = []string{}
	}
}

// ParseEntities parses a JSON string into the Entities field.
func (i *Insight) ParseEntities(s string) {
	_ = json.Unmarshal([]byte(s), &i.Entities)
	if i.Entities == nil {
		i.Entities = []string{}
	}
}

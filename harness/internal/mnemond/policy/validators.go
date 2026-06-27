package policy

import (
	"fmt"
	"strings"

	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

// validatorCatalog is the CLOSED field-validator vocabulary of external spec v2. Each member is a
// compiled behavior the execution switch in compileDecode implements; a spec can only select members
// by id (define≠select). Adding a member is a pure-additive code change to this catalog + the switch.
//
// Member semantics (deny messages are protocol surface, reproduced byte-exactly from the
// pre-data-ization handwritten decoders):
//
//	required {missing_style: empty|missing}  empty processed value → "<style> <field>"
//	format:identifier                        !validIdentifier → "invalid <field>"
//	enum {values: a|b|c, message}            value not in values → "<message>"
//	default {value}                          empty processed value ← value
//	default-from {field}                     empty processed value ← decoded item field (declared earlier)
//	safety:secret                            secret-like → "secret-like content"
//	safety:injection                         injection-shaped → "prompt-injection-shaped content"
//	safety:unsafe                            either of the above → "unsafe content" (combined form)
//	list:strings                             stringSliceField semantics; key omitted when empty;
//	                                         must be the field's only validator
//	list:strings-required                    same list semantics, but empty list denies as "empty <field>";
//	                                         must be the field's only validator
var validatorCatalog = map[string]paramSchema{
	"required":              {required: []string{"missing_style"}},
	"format:identifier":     {},
	"enum":                  {required: []string{"values", "message"}},
	"default":               {required: []string{"value"}},
	"default-from":          {required: []string{"field"}},
	"safety:secret":         {},
	"safety:injection":      {},
	"safety:unsafe":         {},
	"list:strings":          {},
	"list:strings-required": {},
}

// compileDecode builds the EventPackage.Decode closure from typed field policy.
// comment for the frozen decode contract.
func compileDecode(name string, fieldPolicies []FieldSpec) func(payload map[string]any) (Item, error) {
	fields := append([]FieldSpec(nil), fieldPolicies...)
	return func(payload map[string]any) (Item, error) {
		item := Item{}
		for _, f := range fields {
			sectionPayload := payloadSection(payload, f.Section)
			sectionItem := itemSection(item, f.Section)
			if len(f.Validators) == 1 && isListValidator(f.Validators[0].ID) {
				vals := stringSliceField(sectionPayload, f.Name)
				if len(vals) > 0 {
					sectionItem[f.Name] = vals
				}
				if f.Validators[0].ID == "list:strings-required" && len(vals) == 0 {
					return nil, fmt.Errorf("%s candidate denied: empty %s", name, f.Name)
				}
				continue
			}
			raw := strings.TrimSpace(stringField(sectionPayload, f.Name))
			for _, v := range f.Validators {
				switch v.ID {
				case "default":
					if raw == "" {
						raw = v.Params["value"]
					}
				case "default-from":
					if raw == "" {
						raw = itemString(item, v.Params["field"])
					}
				case "required":
					if raw == "" {
						style := "missing"
						if v.Params["missing_style"] == "empty" {
							style = "empty"
						}
						return nil, fmt.Errorf("%s candidate denied: %s %s", name, style, f.Name)
					}
				case "format:identifier":
					if !validIdentifier(raw) {
						return nil, fmt.Errorf("%s candidate denied: invalid %s", name, f.Name)
					}
				case "enum":
					if !enumContains(v.Params["values"], raw) {
						return nil, fmt.Errorf("%s candidate denied: %s", name, v.Params["message"])
					}
				case "safety:secret":
					if containsSecretLikeContent(raw) {
						return nil, fmt.Errorf("%s candidate denied: secret-like content", name)
					}
				case "safety:injection":
					if containsPromptInjectionShape(raw) {
						return nil, fmt.Errorf("%s candidate denied: prompt-injection-shaped content", name)
					}
				case "safety:unsafe":
					if containsSecretLikeContent(raw) || containsPromptInjectionShape(raw) {
						return nil, fmt.Errorf("%s candidate denied: unsafe content", name)
					}
				}
			}
			sectionItem[f.Name] = raw
		}
		return item, nil
	}
}

func payloadSection(payload map[string]any, section string) map[string]any {
	switch section {
	case FieldSectionRule:
		return eventmodel.PayloadRule(payload)
	case FieldSectionNarrative:
		return eventmodel.PayloadNarrative(payload)
	case FieldSectionRefs:
		return eventmodel.PayloadRefs(payload)
	default:
		return nil
	}
}

func itemSection(item Item, section string) map[string]any {
	if existing, ok := item[section].(map[string]any); ok {
		return existing
	}
	created := map[string]any{}
	item[section] = created
	return created
}

func isListValidator(id string) bool {
	return id == "list:strings" || id == "list:strings-required"
}

func enumContains(pipeSeparated, value string) bool {
	for _, v := range strings.Split(pipeSeparated, "|") {
		if v == value {
			return true
		}
	}
	return false
}

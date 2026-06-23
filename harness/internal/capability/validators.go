package capability

import (
	"fmt"
	"strings"
)

// validatorCatalog is the CLOSED field-validator vocabulary of capability spec v1. Each member is a
// compiled behavior the execution switch in compileDecode implements; a spec can only select members
// by id (define≠select). Adding a member is a pure-additive code change to this catalog + the switch.
//
// Member semantics (deny messages are protocol surface, reproduced byte-exactly from the
// pre-data-ization handwritten decoders):
//
//	required {missing_style: empty|missing}  empty processed value → "<style> <field>"
//	format:skill-id                          !validSkillID → "invalid <field>"
//	enum {values: a|b|c, message}            value not in values → "<message>"
//	default {value}                          empty processed value ← value
//	default-from {field}                     empty processed value ← item[field] (declared earlier)
//	safety:secret                            secret-like → "secret-like content"
//	safety:injection                         injection-shaped → "prompt-injection-shaped content"
//	safety:unsafe                            either of the above → "unsafe content" (combined form)
//	list:strings                             stringSliceField semantics; key omitted when empty;
//	                                         must be the field's only validator
//	list:strings-required                    same list semantics, but empty list denies as "empty <field>";
//	                                         must be the field's only validator
var validatorCatalog = map[string]paramSchema{
	"required":              {required: []string{"missing_style"}},
	"format:skill-id":       {},
	"enum":                  {required: []string{"values", "message"}},
	"default":               {required: []string{"value"}},
	"default-from":          {required: []string{"field"}},
	"safety:secret":         {},
	"safety:injection":      {},
	"safety:unsafe":         {},
	"list:strings":          {},
	"list:strings-required": {},
}

// compileDecode builds the Capability.Decode closure from the field specs. See the FromSpec doc
// comment for the frozen decode contract.
func compileDecode(spec CapabilitySpec) func(payload map[string]any) (Item, error) {
	fields := append([]FieldSpec(nil), spec.Fields...)
	name := spec.Name
	return func(payload map[string]any) (Item, error) {
		item := Item{}
		for _, f := range fields {
			if len(f.Validators) == 1 && isListValidator(f.Validators[0].ID) {
				vals := stringSliceField(payload, f.Name)
				if len(vals) > 0 {
					item[f.Name] = vals
				}
				if f.Validators[0].ID == "list:strings-required" && len(vals) == 0 {
					return nil, fmt.Errorf("%s candidate denied: empty %s", name, f.Name)
				}
				continue
			}
			raw := strings.TrimSpace(stringField(payload, f.Name))
			for _, v := range f.Validators {
				switch v.ID {
				case "default":
					if raw == "" {
						raw = v.Params["value"]
					}
				case "default-from":
					if raw == "" {
						raw, _ = item[v.Params["field"]].(string)
					}
				case "required":
					if raw == "" {
						style := "missing"
						if v.Params["missing_style"] == "empty" {
							style = "empty"
						}
						return nil, fmt.Errorf("%s candidate denied: %s %s", name, style, f.Name)
					}
				case "format:skill-id":
					if !validSkillID(raw) {
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
			item[f.Name] = raw
		}
		return item, nil
	}
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

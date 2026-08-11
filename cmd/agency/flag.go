package agency

import "errors"

// singleString preserves the owner-command rule that an effectful option may
// be supplied at most once. Its pflag type remains an ordinary string.
type singleString struct {
	value string
	set   bool
}

func (value *singleString) Set(next string) error {
	if value.set {
		return errors.New("option may only be set once")
	}
	value.value = next
	value.set = true
	return nil
}

func (value *singleString) String() string { return value.value }
func (*singleString) Type() string         { return "string" }

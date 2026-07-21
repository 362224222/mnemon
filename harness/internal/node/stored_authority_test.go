package node

import (
	"context"
	"errors"
	"testing"
)

func TestOpenStoredAuthorityDatabaseFailsClosedWithoutNodeDB(t *testing.T) {
	database, err := openStoredAuthorityDatabase(context.Background(), t.TempDir())
	if database.store != nil || !errors.Is(err, ErrDaemonAuthority) {
		t.Fatalf("missing stored authority = (%#v, %v)", database, err)
	}
}

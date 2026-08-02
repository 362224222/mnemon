package authority

import "errors"

var (
	ErrUnsupportedSchema    = errors.New("authority: unsupported schema")
	ErrWriterActive         = errors.New("authority: writer already active")
	ErrClosed               = errors.New("authority: store closed")
	ErrBootstrapConflict    = errors.New("authority: bootstrap identity conflict")
	ErrOperationConflict    = errors.New("authority: operation key reused with different request")
	ErrArtifactUnavailable  = errors.New("authority: verified Artifact unavailable")
	ErrNoClaimableHandling  = errors.New("authority: no claimable Handling")
	ErrReferenceUnavailable = errors.New("authority: Reference unavailable")
)

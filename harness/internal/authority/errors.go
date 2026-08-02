package authority

import "errors"

var (
	ErrUnsupportedSchema    = errors.New("authority: unsupported schema")
	ErrWriterActive         = errors.New("authority: writer already active")
	ErrClosed               = errors.New("authority: store closed")
	ErrPrincipalUnavailable = errors.New("authority: Principal is not enrolled")
	ErrAttachmentAuth       = errors.New("authority: attachment authentication failed")
	ErrAttachmentExpired    = errors.New("authority: attachment expired")
	ErrOperationConflict    = errors.New("authority: operation key reused with different request")
	ErrArtifactUnavailable  = errors.New("authority: verified Artifact unavailable")
	ErrReferenceUnavailable = errors.New("authority: Reference unavailable")
)

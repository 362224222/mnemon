package authority

import "errors"

var (
	ErrUnsupportedSchema       = errors.New("authority: unsupported schema")
	ErrWriterActive            = errors.New("authority: writer already active")
	ErrClosed                  = errors.New("authority: store closed")
	ErrPrincipalUnavailable    = errors.New("authority: Principal is not enrolled")
	ErrAttachmentAuth          = errors.New("authority: attachment authentication failed")
	ErrAttachmentExpired       = errors.New("authority: attachment expired")
	ErrOperationConflict       = errors.New("authority: operation key reused with different request")
	ErrCurrentUnavailable      = errors.New("authority: Current operation was not issued")
	ErrArtifactUnavailable     = errors.New("authority: verified Artifact unavailable")
	ErrReferenceUnavailable    = errors.New("authority: Reference unavailable")
	ErrPeerRouteUnavailable    = errors.New("authority: peer route unavailable")
	ErrPeerRouteConflict       = errors.New("authority: peer route conflicts with immutable enrollment")
	ErrPeerRouteRevoked        = errors.New("authority: peer route is revoked")
	ErrPeerAuthentication      = errors.New("authority: peer authentication failed")
	ErrPeerInboxBound          = errors.New("authority: staged peer delivery bound reached")
	ErrPeerDeliveryConflict    = errors.New("authority: peer delivery replay conflicts")
	ErrPeerDeliveryExpired     = errors.New("authority: peer delivery is no longer live")
	ErrPeerDeliveryUnavailable = errors.New("authority: peer delivery unavailable")
)

package driver

import multicasurface "github.com/mnemon-dev/mnemon/harness/internal/surface/multica"

const (
	MulticaMetadataEventRef          = multicasurface.MulticaMetadataEventRef
	MulticaMetadataResourceRef       = multicasurface.MulticaMetadataResourceRef
	MulticaMetadataProjectionRef     = multicasurface.MulticaMetadataProjectionRef
	MulticaMetadataSourceArtifactRef = multicasurface.MulticaMetadataSourceArtifactRef
	MulticaMetadataSurfaceRole       = multicasurface.MulticaMetadataSurfaceRole
	MulticaMetadataNoAutoDispatch    = multicasurface.MulticaMetadataNoAutoDispatch
	MulticaMetadataCaseID            = multicasurface.MulticaMetadataCaseID
	MulticaMetadataRound             = multicasurface.MulticaMetadataRound
	MulticaMetadataPoCID             = multicasurface.MulticaMetadataPoCID

	MulticaSurfaceRoleDisplay  = string(multicasurface.SurfaceRoleDisplay)
	MulticaSurfaceRoleActivate = string(multicasurface.SurfaceRoleActivate)
	MulticaSurfaceRoleInput    = string(multicasurface.SurfaceRoleInput)
	MulticaSurfaceRoleDrift    = string(multicasurface.SurfaceRoleDrift)
)

type MulticaSurfaceRefs = multicasurface.SurfaceRefs

func ParseMulticaSurfaceRefs(raw map[string]any) MulticaSurfaceRefs {
	return multicasurface.ParseSurfaceRefs(raw)
}

func MulticaIssueSurfaceRefs(issue MulticaIssue) MulticaSurfaceRefs {
	return ParseMulticaSurfaceRefs(issue.Metadata)
}

func NormalizeMulticaMetadata(raw any) map[string]string {
	return multicasurface.NormalizeMulticaMetadata(raw)
}

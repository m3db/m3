package schema

// VersionChecker centralizes logic for checking if a major, minor version combo supports
// specific functionality
type VersionChecker struct {
	majorVersion int
	minorVersion int
}

func NewVersionChecker(majorVersion int, minorVersion int) *VersionChecker {
	return &VersionChecker{
		majorVersion: majorVersion,
		minorVersion: minorVersion,
	}
}

// IndexEntryValidationEnabled checks the version to determine if
// fileset files of the specified version allow for doing checksum validation
// on individual index entries
func (v *VersionChecker) IndexEntryValidationEnabled() bool {
	return v.majorVersion >= 1 && v.minorVersion >= 1
}

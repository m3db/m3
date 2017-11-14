package handler

const (
	// WarningsHeader is the M3 warnings header when to display a warning to a user
	WarningsHeader = "M3-Warnings"

	// RetryHeader is the M3 retry header to display when it is safe to retry
	RetryHeader = "M3-Retry"

	// ServedByHeader is the M3 query storage execution breakdown
	ServedByHeader = "M3-Storage-By"

	// DeprecatedHeader is the M3 deprecated header
	DeprecatedHeader = "M3-Deprecated"
)

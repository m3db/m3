package graphite

// ExtractNthMetricPart returns the nth part of the metric string. Index starts from 0
// and assumes metrics are delimited by '.'. If n is negative or bigger than the number
// of parts, returns an empty string.
func ExtractNthMetricPart(metric string, n int) string {
	return ExtractNthStringPart(metric, n, '.')
}

// ExtractNthStringPart returns the nth part of the metric string. Index starts from 0.
// If n is negative or bigger than the number of parts, returns an empty string.
func ExtractNthStringPart(target string, n int, delim rune) string {
	if n < 0 {
		return ""
	}

	leftSide := 0
	delimsToGo := n + 1
	for i := 0; i < len(target); i++ {
		if target[i] == byte(delim) {
			delimsToGo--
			if delimsToGo == 0 {
				return target[leftSide:i]
			}
			leftSide = i + 1
		}
	}

	if delimsToGo > 1 {
		return ""
	}

	return target[leftSide:]
}

package topology

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

// nolint: dupl
func TestConsistencyLevel_UnmarshalYAML(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    ConsistencyLevel
		expectError bool
		errorMsg    string
	}{
		{
			name:     "Valid consistency level",
			input:    "one",
			expected: ConsistencyLevelOne,
		},
		{
			name:        "Invalid read consistency level",
			input:       "InvalidLevel",
			expectError: true,
			errorMsg:    "invalid ConsistencyLevel 'InvalidLevel'",
		},
		{
			name:        "Empty string should fail",
			input:       "",
			expectError: true,
			errorMsg:    errConsistencyLevelUnspecified.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var level ConsistencyLevel
			data := fmt.Sprintf("\"%s\"", tt.input)
			err := yaml.Unmarshal([]byte(data), &level)

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, level)
		})
	}
}

// nolint: dupl
func TestReadConsistencyLevel_UnmarshalYAML(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    ReadConsistencyLevel
		expectError bool
		errorMsg    string
	}{
		{
			name:     "Valid read consistency level",
			input:    "unstrict_majority",
			expected: ReadConsistencyLevelUnstrictMajority,
		},
		{
			name:        "Invalid read consistency level",
			input:       "InvalidLevel",
			expectError: true,
			errorMsg:    "invalid ReadConsistencyLevel 'InvalidLevel'",
		},
		{
			name:        "Empty string should fail",
			input:       "",
			expectError: true,
			errorMsg:    errReadConsistencyLevelUnspecified.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var level ReadConsistencyLevel
			data := fmt.Sprintf("\"%s\"", tt.input)
			err := yaml.Unmarshal([]byte(data), &level)

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, level)
		})
	}
}

// nolint: dupl
func TestConnectConsistencyLevel_UnmarshalYAML(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    ConnectConsistencyLevel
		expectError bool
		errorMsg    string
	}{
		{
			name:     "Valid read consistency level",
			input:    "majority",
			expected: ConnectConsistencyLevelMajority,
		},
		{
			name:        "Invalid read consistency level",
			input:       "InvalidLevel",
			expectError: true,
			errorMsg:    "invalid ConnectConsistencyLevel 'InvalidLevel'",
		},
		{
			name:        "Empty string should fail",
			input:       "",
			expectError: true,
			errorMsg:    errConsistencyLevelUnspecified.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var level ConnectConsistencyLevel
			data := fmt.Sprintf("\"%s\"", tt.input)
			err := yaml.Unmarshal([]byte(data), &level)

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, level)
		})
	}
}

func TestNumDesiredForReadConsistency(t *testing.T) {
	tests := []struct {
		name        string
		level       ReadConsistencyLevel
		numReplicas int
		majority    int
		expected    int
		expectPanic bool
	}{
		{"ReadConsistencyLevelAll", ReadConsistencyLevelAll, 5, 3, 5, false},
		{"ReadConsistencyLevelUnstrictAll", ReadConsistencyLevelUnstrictAll, 4, 2, 4, false},
		{"ReadConsistencyLevelMajority", ReadConsistencyLevelMajority, 6, 3, 3, false},
		{"ReadConsistencyLevelUnstrictMajority", ReadConsistencyLevelUnstrictMajority, 7, 4, 4, false},
		{"ReadConsistencyLevelOne", ReadConsistencyLevelOne, 10, 5, 1, false},
		{"ReadConsistencyLevelNone", ReadConsistencyLevelNone, 8, 4, 0, false},
		{"Invalid ReadConsistencyLevel", ReadConsistencyLevel(999), 5, 3, 0, true}, // Expect panic
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectPanic {
				require.Panics(t, func() {
					NumDesiredForReadConsistency(tt.level, tt.numReplicas, tt.majority)
				})
			} else {
				result := NumDesiredForReadConsistency(tt.level, tt.numReplicas, tt.majority)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestReadConsistencyAchieved(t *testing.T) {
	tests := []struct {
		name        string
		level       ReadConsistencyLevel
		majority    int
		numPeers    int
		numSuccess  int
		expected    bool
		expectPanic bool
	}{
		{"All - Achieved", ReadConsistencyLevelAll, 3, 5, 5, true, false},
		{"All - Not Achieved", ReadConsistencyLevelAll, 3, 5, 4, false, false},

		{"Majority - Achieved", ReadConsistencyLevelMajority, 3, 5, 3, true, false},
		{"Majority - Not Achieved", ReadConsistencyLevelMajority, 3, 5, 2, false, false},

		{"One - Achieved", ReadConsistencyLevelOne, 3, 5, 1, true, false},
		{"One - Not Achieved", ReadConsistencyLevelOne, 3, 5, 0, false, false},

		{"Unstrict Majority - Achieved", ReadConsistencyLevelUnstrictMajority, 3, 5, 1, true, false},
		{"Unstrict All - Achieved", ReadConsistencyLevelUnstrictAll, 3, 5, 1, true, false},

		{"None - Always True", ReadConsistencyLevelNone, 3, 5, 0, true, false},

		{"Invalid Level - Should Panic", ReadConsistencyLevel(999), 3, 5, 2, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectPanic {
				require.Panics(t, func() {
					ReadConsistencyAchieved(tt.level, tt.majority, tt.numPeers, tt.numSuccess)
				})
			} else {
				result := ReadConsistencyAchieved(tt.level, tt.majority, tt.numPeers, tt.numSuccess)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestReadConsistencyTermination(t *testing.T) {
	tests := []struct {
		name        string
		level       ReadConsistencyLevel
		majority    int32
		remaining   int32
		success     int32
		expected    bool
		expectPanic bool
	}{
		// ReadConsistencyLevelOne
		{"One - Success Achieved", ReadConsistencyLevelOne, 3, 2, 1, true, false},
		{"One - No Success, Not Done", ReadConsistencyLevelOne, 3, 2, 0, false, false},
		{"One - No Success, Done", ReadConsistencyLevelOne, 3, 0, 0, true, false},

		// ReadConsistencyLevelNone
		{"None - Success Achieved", ReadConsistencyLevelNone, 3, 2, 1, true, false},
		{"None - No Success, Not Done", ReadConsistencyLevelNone, 3, 2, 0, false, false},
		{"None - No Success, Done", ReadConsistencyLevelNone, 3, 0, 0, true, false},

		// ReadConsistencyLevelMajority
		{"Majority - Success Achieved", ReadConsistencyLevelMajority, 3, 2, 3, true, false},
		{"Majority - No Success, Not Done", ReadConsistencyLevelMajority, 3, 2, 2, false, false},
		{"Majority - No Success, Done", ReadConsistencyLevelMajority, 3, 0, 2, true, false},

		// ReadConsistencyLevelUnstrictMajority
		{"Unstrict Majority - Success Achieved", ReadConsistencyLevelUnstrictMajority, 3, 2, 3, true, false},
		{"Unstrict Majority - No Success, Not Done", ReadConsistencyLevelUnstrictMajority, 3, 2, 2, false, false},
		{"Unstrict Majority - No Success, Done", ReadConsistencyLevelUnstrictMajority, 3, 0, 2, true, false},

		// ReadConsistencyLevelAll
		{"All - Not Done", ReadConsistencyLevelAll, 3, 2, 3, false, false},
		{"All - Done", ReadConsistencyLevelAll, 3, 0, 3, true, false},

		// ReadConsistencyLevelUnstrictAll
		{"Unstrict All - Not Done", ReadConsistencyLevelUnstrictAll, 3, 2, 3, false, false},
		{"Unstrict All - Done", ReadConsistencyLevelUnstrictAll, 3, 0, 3, true, false},

		// Invalid Level
		{"Invalid Level - Should Panic", ReadConsistencyLevel(999), 3, 2, 1, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectPanic {
				require.Panics(t, func() {
					ReadConsistencyTermination(tt.level, tt.majority, tt.remaining, tt.success)
				}, "Expected function to panic for invalid ReadConsistencyLevel")
			} else {
				result := ReadConsistencyTermination(tt.level, tt.majority, tt.remaining, tt.success)
				require.Equal(t, tt.expected, result, "Unexpected result for %v", tt.level)
			}
		})
	}
}

func TestWriteConsistencyAchieved(t *testing.T) {
	tests := []struct {
		name        string
		level       ConsistencyLevel
		majority    int
		numPeers    int
		numSuccess  int
		expected    bool
		expectPanic bool
	}{
		// ConsistencyLevelAll
		{"All - All Success", ConsistencyLevelAll, 3, 3, 3, true, false},
		{"All - Not All Success", ConsistencyLevelAll, 3, 3, 2, false, false},

		// ConsistencyLevelMajority
		{"Majority - Meets Majority", ConsistencyLevelMajority, 2, 3, 2, true, false},
		{"Majority - Does Not Meet Majority", ConsistencyLevelMajority, 2, 3, 1, false, false},

		// ConsistencyLevelOne
		{"One - At Least One Success", ConsistencyLevelOne, 3, 3, 1, true, false},
		{"One - No Success", ConsistencyLevelOne, 3, 3, 0, false, false},

		// Invalid Level
		{"Invalid Level - Should Panic", ConsistencyLevel(999), 3, 3, 1, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectPanic {
				require.Panics(t, func() {
					WriteConsistencyAchieved(tt.level, tt.majority, tt.numPeers, tt.numSuccess)
				}, "Expected function to panic for invalid ConsistencyLevel")
			} else {
				result := WriteConsistencyAchieved(tt.level, tt.majority, tt.numPeers, tt.numSuccess)
				require.Equal(t, tt.expected, result, "Unexpected result for %v", tt.level)
			}
		})
	}
}

func TestValidateReadConsistencyLevel(t *testing.T) {
	tests := []struct {
		name        string
		level       ReadConsistencyLevel
		expectedErr error
	}{
		{"Valid - ReadConsistencyLevelNone", ReadConsistencyLevelNone, nil},
		{"Invalid - Unknown Level", ReadConsistencyLevel(999), errReadConsistencyLevelInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateReadConsistencyLevel(tt.level)
			require.Equal(t, tt.expectedErr, err, "Unexpected error for level: %v", tt.level)
		})
	}
}

func TestParseReadConsistencyLevel(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    ReadConsistencyLevel
		expectedErr error
	}{
		{"Valid - ReadConsistencyLevelAll", "all", ReadConsistencyLevelAll, nil},
		{"Invalid - Empty String", "", ReadConsistencyLevel(0), errConsistencyLevelUnspecified},
		{"Invalid - Random String", "InvalidLevel", ReadConsistencyLevel(0),
			fmt.Errorf("invalid ReadConsistencyLevel 'InvalidLevel' valid types are: %v", ValidReadConsistencyLevels())},
		{"Invalid - Case Sensitive", "readconsistencylevelall", ReadConsistencyLevel(0),
			fmt.Errorf("invalid ReadConsistencyLevel 'readconsistencylevelall' valid types are: %v",
				ValidReadConsistencyLevels())},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseReadConsistencyLevel(tt.input)

			if tt.expectedErr != nil {
				require.Error(t, err, "Expected error but got nil")
				require.Contains(t, err.Error(), tt.expectedErr.Error(), "Unexpected error message")
			} else {
				require.NoError(t, err, "Unexpected error for input: %s", tt.input)
				require.Equal(t, tt.expected, result, "Unexpected result for input: %s", tt.input)
			}
		})
	}
}

func TestValidateConnectConsistencyLevel(t *testing.T) {
	tests := []struct {
		name        string
		level       ConnectConsistencyLevel
		expectedErr error
	}{
		{"Valid - ConnectConsistencyLevelNone", ConnectConsistencyLevelAny, nil},
		{"Invalid - Unknown Level", ConnectConsistencyLevel(999), errClusterConnectConsistencyLevelInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConnectConsistencyLevel(tt.level)
			require.Equal(t, tt.expectedErr, err, "Unexpected error for level: %v", tt.level)
		})
	}
}

func TestValidateConsistencyLevel(t *testing.T) {
	tests := []struct {
		name        string
		level       ConsistencyLevel
		expectedErr error
	}{
		{"Valid - ConsistencyLevelOne", ConsistencyLevelOne, nil},
		{"Invalid - Unknown Level", ConsistencyLevel(999), errConsistencyLevelInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConsistencyLevel(tt.level)
			require.Equal(t, tt.expectedErr, err, "Unexpected error for level: %v", tt.level)
		})
	}
}

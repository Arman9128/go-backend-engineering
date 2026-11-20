package task

import "testing"

func TestIsValidStatus(t *testing.T) {
	validStatuses := []string{
		StatusPending,
		StatusProcessing,
		StatusCompleted,
		StatusFailed,
		StatusRejected,
	}

	for _, status := range validStatuses {
		if !IsValidStatus(status) {
			t.Errorf("expected status %q to be valid", status)
		}
	}
}

func TestIsValidStatusRejectsUnknownValue(t *testing.T) {
	if IsValidStatus("unknown") {
		t.Error("expected unknown status to be invalid")
	}
}

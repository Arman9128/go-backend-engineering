package user

import "testing"

func TestNewUser(t *testing.T) {
	newUser := NewUser(1, "Arman", "arman@example.com")

	if newUser.ID != 1 {
		t.Errorf("expected ID 1, got %d", newUser.ID)
	}

	if newUser.Name != "Arman" {
		t.Errorf("expected name Arman, got %s", newUser.Name)
	}

	if newUser.Email != "arman@example.com" {
		t.Errorf("expected email arman@example.com, got %s", newUser.Email)
	}
}

func TestHasPassword(t *testing.T) {
	newUser := NewUser(1, "Arman", "arman@example.com")

	if !newUser.HasPassword() {
		t.Error("expected HasPassword() to return true")
	}
}

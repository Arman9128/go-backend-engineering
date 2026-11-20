package main

import (
	"errors"
	"testing"
)

func TestFindUserNotFound(t *testing.T) {
	err := findUser(15)

	if !errors.Is(err, ErrUserNotFound) {
		t.Errorf("expected ErrUserNotFound, got %v", err)
	}
}

func TestFindUserSuccess(t *testing.T) {
	err := findUser(1)

	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestGetUserWrappedError(t *testing.T) {
	err := getUser(15)

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, ErrUserNotFound) {
		t.Errorf("expected wrapped ErrUserNotFound, got %v", err)
	}
}

func TestGetUserErrorMessage(t *testing.T) {
	err := getUser(15)

	expected := "get user 15: user not found"

	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
}

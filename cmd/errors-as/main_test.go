package main

import (
	"errors"
	"testing"
)

func TestFindUserReturnsHTTPError(t *testing.T) {
	err := findUser(15)

	var httpErr *HTTPError

	if !errors.As(err, &httpErr) {
		t.Fatal("expected HTTPError")
	}

	if httpErr.StatusCode != 404 {
		t.Errorf("expected status 404, got %d", httpErr.StatusCode)
	}
}

func TestGetUserReturnsWrappedHTTPError(t *testing.T) {
	err := getUser(15)

	var httpErr *HTTPError

	if !errors.As(err, &httpErr) {
		t.Fatal("expected wrapped HTTPError")
	}

	if httpErr.Message != "user not found" {
		t.Errorf("unexpected message: %s", httpErr.Message)
	}
}

func TestGetUserSuccess(t *testing.T) {
	err := getUser(10)

	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestErrorsAsWithDifferentErrorType(t *testing.T) {
	err := errors.New("database connection failed")

	var httpErr *HTTPError

	if errors.As(err, &httpErr) {
		t.Fatal("expected errors.As to return false")
	}
}

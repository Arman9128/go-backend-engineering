package main

import (
	"errors"
	"testing"
)

type MockUserRepository struct {
	user User
	err  error
}

func (r *MockUserRepository) FindByID(id int) (User, error) {
	return r.user, r.err
}

func TestGetUser(t *testing.T) {
	mock := &MockUserRepository{
		user: User{
			ID:    1,
			Name:  "Arman",
			Email: "arman@example.com",
		},
	}

	service := NewUserService(mock)

	user, err := service.GetUser(1)
	if err != nil {
		t.Fatal(err)
	}

	if user.Name != "Arman" {
		t.Errorf("expected Arman, got %s", user.Name)
	}
}

func TestGetUserError(t *testing.T) {
	mock := &MockUserRepository{
		err: errors.New("database error"),
	}

	service := NewUserService(mock)

	_, err := service.GetUser(1)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
}

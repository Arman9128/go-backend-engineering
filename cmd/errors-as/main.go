package main

import (
	"errors"
	"fmt"
)

type HTTPError struct {
	StatusCode int
	Message    string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Message)
}

func findUser(id int) error {
	if id == 15 {
		return &HTTPError{
			StatusCode: 404,
			Message:    "user not found",
		}
	}

	return nil
}

func getUser(id int) error {
	err := findUser(id)
	if err != nil {
		return fmt.Errorf("get user %d: %w", id, err)
	}

	return nil
}

func main() {
	err := getUser(15)

	if err != nil {
		fmt.Println("Error:", err)

		var httpErr *HTTPError

		if errors.As(err, &httpErr) {
			fmt.Println("HTTP Status:", httpErr.StatusCode)
			fmt.Println("Message:", httpErr.Message)
		}
	} else {
		fmt.Println("User found successfully")
	}

	plainErr := errors.New("database connection failed")

	var notFoundErr *HTTPError

	if errors.As(plainErr, &notFoundErr) {
		fmt.Println("HTTP Status:", notFoundErr.StatusCode)
	} else {
		fmt.Println("Not an HTTPError")
	}
}

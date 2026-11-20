package main

import (
	"errors"
	"fmt"
)

var ErrUserNotFound = errors.New("user not found")

func findUser(id int) error {
	if id == 15 {
		return ErrUserNotFound
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
	fmt.Println("=== Error Wrapping ===")

	err := getUser(15)

	if err != nil {
		fmt.Println("Error:", err)
	}

	fmt.Println("\n=== errors.Is ===")
	fmt.Println(errors.Is(err, ErrUserNotFound))

	fmt.Printf("\n=== %%v vs %%w ===\n")

	wrappedError := fmt.Errorf("get user: %w", ErrUserNotFound)
	plainError := fmt.Errorf("get user: %v", ErrUserNotFound)

	fmt.Printf("With %%w: %t\n", errors.Is(wrappedError, ErrUserNotFound))
	fmt.Printf("With %%v: %t\n", errors.Is(plainError, ErrUserNotFound))
}

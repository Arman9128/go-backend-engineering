package main

import "fmt"

type User struct {
	ID         int
	Name       string
	Email      string
	Age        int
	IsActive   bool
	Experience float64
}

func main() {
	user := User{
		ID:         1,
		Name:       "Arman",
		Email:      "arman@example.com",
		Age:        35,
		IsActive:   true,
		Experience: 7.5,
	}

	fmt.Println("=== User API Response ===")

	fmt.Printf("ID: %d\n", user.ID)
	fmt.Printf("Name: %s\n", user.Name)
	fmt.Printf("Email: %s\n", user.Email)
	fmt.Printf("Age: %d\n", user.Age)
	fmt.Printf("Active: %t\n", user.IsActive)
	fmt.Printf("Experience: %.1f years\n", user.Experience)
}

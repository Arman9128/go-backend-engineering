package main

import "fmt"

const (
	appName    = "Go Backend Engineering"
	appVersion = "1.0.0"
	maxRetries = 3
)

var environment = "development"

func main() {
	var age int
	var name string
	var isActive bool

	userName := "Arman"

	fmt.Println("=== Application ===")
	fmt.Println("Name:", appName)
	fmt.Println("Version:", appVersion)
	fmt.Println("Environment:", environment)
	fmt.Println("Max retries:", maxRetries)

	fmt.Println("\n=== Zero Values ===")
	fmt.Printf("Age: %d\n", age)
	fmt.Printf("Name: %q\n", name)
	fmt.Printf("Active: %t\n", isActive)

	fmt.Println("\n=== Scope ===")
	fmt.Println("User:", userName)
}

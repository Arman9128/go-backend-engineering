package main

import "fmt"

func main() {
	var age int = 25
	var isAuthenticated bool = true
	var isAdmin bool = false

	fmt.Println("=== Access Control ===")

	if age < 18 {
		fmt.Println("Access denied: underage")
	} else if !isAuthenticated {
		fmt.Println("Access denied: authentication required")
	} else {
		fmt.Println("Access granted")
	}

	if isAdmin {
		fmt.Println("Admin privileges enabled")
	} else {
		fmt.Println("Regular user")
	}

	if age >= 18 && isAuthenticated {
		fmt.Println("Can access protected resources")
	}

	if !isAdmin || !isAuthenticated {
		fmt.Println("Restricted administrative access")
	}
}

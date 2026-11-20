package main

import (
	"fmt"

	"github.com/Arman9128/go-backend-engineering/internal/user"
)

func main() {
	newUser := user.NewUser(1, "Arman", "arman@example.com")

	fmt.Println("ID:", newUser.ID)
	fmt.Println("Name:", newUser.Name)
	fmt.Println("Email:", newUser.Email)
	fmt.Println("Has password:", newUser.HasPassword())
}

package main

import "fmt"

type User struct {
	Name string
	Age  int
}

// Обычная функция
func printUser(user User) {
	fmt.Printf("Function: %s, age %d\n", user.Name, user.Age)
}

// Value receiver — работает с копией
func (u User) BirthdayValue() {
	u.Age++
	fmt.Println("Inside BirthdayValue:", u.Age)
}

// Pointer receiver — изменяет оригинал
func (u *User) BirthdayPointer() {
	u.Age++
	fmt.Println("Inside BirthdayPointer:", u.Age)
}

func main() {
	user := User{
		Name: "Arman",
		Age:  35,
	}

	fmt.Println("=== Ordinary Function ===")
	printUser(user)

	fmt.Println("\n=== Value Receiver ===")
	user.BirthdayValue()
	fmt.Println("Original age:", user.Age)

	fmt.Println("\n=== Pointer Receiver ===")
	user.BirthdayPointer()
	fmt.Println("Original age:", user.Age)
}

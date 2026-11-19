package main

import "fmt"

func main() {
	var name string = "Arman"
	var age int = 35
	var experience float64 = 7.5
	var isDeveloper bool = true

	country := "Armenia"
	salary := 2500

	fmt.Println("=== User Profile ===")

	fmt.Printf("Name: %s\n", name)
	fmt.Printf("Age: %d\n", age)
	fmt.Printf("Experience: %.1f years\n", experience)
	fmt.Printf("Developer: %t\n", isDeveloper)
	fmt.Printf("Country: %s\n", country)
	fmt.Printf("Salary: %d USD\n", salary)

	fmt.Println("\n=== Data Types ===")

	fmt.Printf("name: %T\n", name)
	fmt.Printf("age: %T\n", age)
	fmt.Printf("experience: %T\n", experience)
	fmt.Printf("isDeveloper: %T\n", isDeveloper)
	fmt.Printf("country: %T\n", country)
	fmt.Printf("salary: %T\n", salary)
}

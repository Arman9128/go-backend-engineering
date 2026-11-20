package main

import "fmt"

func updateAge(age *int) {
	if age == nil {
		fmt.Println("Age pointer is nil")
		return
	}

	*age = *age + 1
}

func main() {
	age := 35
	pointer := &age

	fmt.Println("=== Pointer Basics ===")
	fmt.Println("Value:", age)
	fmt.Println("Address:", pointer)
	fmt.Println("Value through pointer:", *pointer)

	*pointer = 36
	fmt.Println("Updated age:", age)

	fmt.Println("\n=== Pointer Function ===")
	updateAge(pointer)
	fmt.Println("Age after update:", age)

	fmt.Println("\n=== Nil Pointer ===")
	var nilPointer *int

	updateAge(nilPointer)

	fmt.Println("Nil pointer:", nilPointer)
}

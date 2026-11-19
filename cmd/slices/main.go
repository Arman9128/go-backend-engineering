package main

import "fmt"

func main() {
	numbers := make([]int, 3, 5)

	numbers[0] = 10
	numbers[1] = 20
	numbers[2] = 30

	fmt.Println("Numbers:", numbers)
	fmt.Println("Length:", len(numbers))
	fmt.Println("Capacity:", cap(numbers))

	numbers = append(numbers, 40)

	fmt.Println("\nAfter append:")
	fmt.Println("Numbers:", numbers)
	fmt.Println("Length:", len(numbers))
	fmt.Println("Capacity:", cap(numbers))
}

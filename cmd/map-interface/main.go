package main

import "fmt"

func main() {
	user := map[string]any{
		"name":       "Arman",
		"age":        35,
		"experience": 7.5,
		"is_active":  true,
	}

	fmt.Println("=== User Data ===")

	for key, value := range user {
		fmt.Printf("%s: %v (type: %T)\n", key, value, value)
	}

	fmt.Println("\n=== Type Assertions ===")

	name, nameOK := user["name"].(string)
	if nameOK {
		fmt.Println("Name:", name)
	}

	age, ageOK := user["age"].(int)
	if ageOK {
		fmt.Println("Age next year:", age+1)
	}

	experience, experienceOK := user["experience"].(float64)
	if experienceOK {
		fmt.Println("Experience:", experience)
	}

	fmt.Println("\n=== Incorrect Type ===")

	wrongAge, ok := user["age"].(string)

	if ok {
		fmt.Println("Age:", wrongAge)
	} else {
		fmt.Println("Age is not a string")
	}

	fmt.Println("\n=== Missing Key ===")

	email, exists := user["email"]

	if exists {
		fmt.Println("Email:", email)
	} else {
		fmt.Println("Email not found")
	}
}

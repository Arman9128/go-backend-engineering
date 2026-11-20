package main

import (
	"errors"
	"fmt"
)

type User struct {
	ID    int
	Name  string
	Email string
}

// Контракт для любого хранилища пользователей.
type UserRepository interface {
	FindByID(id int) (User, error)
}

// Реализация хранилища в памяти.
type MemoryUserRepository struct {
	users map[int]User
}

func (r *MemoryUserRepository) FindByID(id int) (User, error) {
	user, exists := r.users[id]
	if !exists {
		return User{}, errors.New("user not found")
	}

	return user, nil
}

// Бизнес-логика зависит от интерфейса, а не от конкретного хранилища.
type UserService struct {
	repository UserRepository
}

func NewUserService(repository UserRepository) *UserService {
	return &UserService{
		repository: repository,
	}
}

func (s *UserService) GetUser(id int) (User, error) {
	return s.repository.FindByID(id)
}

func main() {
	repository := &MemoryUserRepository{
		users: map[int]User{
			1: {
				ID:    1,
				Name:  "Arman",
				Email: "arman@example.com",
			},
		},
	}

	// Передаём конкретную реализацию через интерфейс.
	service := NewUserService(repository)

	user, err := service.GetUser(1)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Printf("User: %+v\n", user)
}

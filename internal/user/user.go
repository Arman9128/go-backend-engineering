package user

type User struct {
	ID       int
	Name     string
	Email    string
	password string
}

func NewUser(id int, name string, email string) User {
	return User{
		ID:       id,
		Name:     name,
		Email:    email,
		password: "secret123",
	}
}

func (u User) HasPassword() bool {
	return u.password != ""
}

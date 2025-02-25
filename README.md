```
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gorilla/mux"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var db *gorm.DB

// User struct representing the users table
type User struct {
	ID    uint   `json:"id" gorm:"primaryKey;autoIncrement"`
	Name  string `json:"name" gorm:"not null"`
	Email string `json:"email" gorm:"unique;not null"`
}

func initDB() {
	var err error
	// Change this DSN as per your MySQL setup
	dsn := "root:Kush@789#@tcp(dev.wikibedtimestories.com:31347)/testdb?charset=utf8mb4&parseTime=True&loc=Local"
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Error connecting to the database", err)
	}

	// AutoMigrate creates the users table if it doesn't exist
	db.AutoMigrate(&User{})
}

// CreateUser handles POST request to insert a new user
func CreateUser(w http.ResponseWriter, r *http.Request) {
	var user User
	if err := json.NewDecoder(r.Body).Decode(&user); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	if err := db.Create(&user).Error; err != nil {
		http.Error(w, "Error inserting user", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(user)
}

func main() {
	initDB()

	r := mux.NewRouter()
	r.HandleFunc("/users", CreateUser).Methods("POST")

	fmt.Println("Server started at :8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}

// Test cases using sqlmock
func TestCreateUser(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error initializing mock DB: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectExec("INSERT INTO `users`").
		WithArgs("John Doe", "john@example.com").
		WillReturnResult(sqlmock.NewResult(1, 1))

	reqBody := `
	{
	"name": "John Doe", 
	"email": "john@example.com"
	}
	`
	req, err := http.NewRequest("POST", "/users", strings.NewReader(reqBody))
	if err != nil {
		t.Fatalf("Error creating request: %v", err)
	}

	w := &mockResponseWriter{headers: http.Header{}}
	CreateUser(w, req)

	if w.statusCode != http.StatusCreated {
		t.Errorf("Expected status code %d, got %d", http.StatusCreated, w.statusCode)
	}
}

type mockResponseWriter struct {
	headers    http.Header
	body       []byte
	statusCode int
}

func (m *mockResponseWriter) Header() http.Header {
	return m.headers
}

func (m *mockResponseWriter) Write(b []byte) (int, error) {
	m.body = append(m.body, b...)
	return len(b), nil
}

func (m *mockResponseWriter) WriteHeader(statusCode int) {
	m.statusCode = statusCode
}

```

package main

import (
	"log"

	"github.com/twojanazwa/aegis-edge/internal/database"
)

func main() {
	db, err := database.NewPostgres(
		"host=localhost port=5432 user=aegis password=aegis dbname=aegis sslmode=disable",
	)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	log.Println("Connected to PostgreSQL!")
}

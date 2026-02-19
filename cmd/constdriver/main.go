package main

import (
	"database/sql"
	"fmt"

	_ "github.com/replu/goconmini-sendai-2026/constdriver"
)

func main() {
	db, err := sql.Open("const-driver", "")
	if err != nil {
		panic(err)
	}

	stmt, err := db.Prepare("")
	if err != nil {
		panic(err)
	}

	rows, err := stmt.Query("")
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		fmt.Println(id, name)
	}
	if err := rows.Close(); err != nil {
		panic(err)
	}
}

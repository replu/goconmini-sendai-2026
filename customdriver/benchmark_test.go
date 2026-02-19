package customdriver

import (
	"context"
	"database/sql"
	"testing"
)

func benchMySQLExecQuery(b *testing.B, db *sql.DB) {
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		result, err := db.ExecContext(ctx, "INSERT INTO users (name) VALUES (?)", "bench")
		if err != nil {
			b.Fatalf("INSERT failed: %v", err)
		}
		id, _ := result.LastInsertId()

		var name string
		if err := db.QueryRowContext(ctx, "SELECT name FROM users WHERE id = ?", id).Scan(&name); err != nil {
			b.Fatalf("SELECT failed: %v", err)
		}

		if _, err := db.ExecContext(ctx, "DELETE FROM users WHERE id = ?", id); err != nil {
			b.Fatalf("DELETE failed: %v", err)
		}
	}
}

func benchMySQLStmt(b *testing.B, db *sql.DB) {
	ctx := context.Background()

	insertStmt, err := db.PrepareContext(ctx, "INSERT INTO users (name) VALUES (?)")
	if err != nil {
		b.Fatalf("Prepare INSERT failed: %v", err)
	}
	defer insertStmt.Close()

	selectStmt, err := db.PrepareContext(ctx, "SELECT name FROM users WHERE id = ?")
	if err != nil {
		b.Fatalf("Prepare SELECT failed: %v", err)
	}
	defer selectStmt.Close()

	deleteStmt, err := db.PrepareContext(ctx, "DELETE FROM users WHERE id = ?")
	if err != nil {
		b.Fatalf("Prepare DELETE failed: %v", err)
	}
	defer deleteStmt.Close()

	b.ResetTimer()
	for b.Loop() {
		result, err := insertStmt.ExecContext(ctx, "bench")
		if err != nil {
			b.Fatalf("stmt INSERT failed: %v", err)
		}
		id, _ := result.LastInsertId()

		var name string
		if err := selectStmt.QueryRowContext(ctx, id).Scan(&name); err != nil {
			b.Fatalf("stmt SELECT failed: %v", err)
		}

		if _, err := deleteStmt.ExecContext(ctx, id); err != nil {
			b.Fatalf("stmt DELETE failed: %v", err)
		}
	}
}

func BenchmarkMySQL_RawDriver_ExecQuery(b *testing.B) {
	benchMySQLExecQuery(b, rawMySQLDB)
}

func BenchmarkMySQL_CustomDriver_ExecQuery(b *testing.B) {
	benchMySQLExecQuery(b, testMySQLDB)
}

func BenchmarkMySQL_RawDriver_Stmt(b *testing.B) {
	benchMySQLStmt(b, rawMySQLDB)
}

func BenchmarkMySQL_CustomDriver_Stmt(b *testing.B) {
	benchMySQLStmt(b, testMySQLDB)
}

func benchPgExecQuery(b *testing.B, db *sql.DB) {
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		var id int64
		if err := db.QueryRowContext(ctx, "INSERT INTO users (name) VALUES ($1) RETURNING id", "bench").Scan(&id); err != nil {
			b.Fatalf("INSERT failed: %v", err)
		}

		var name string
		if err := db.QueryRowContext(ctx, "SELECT name FROM users WHERE id = $1", id).Scan(&name); err != nil {
			b.Fatalf("SELECT failed: %v", err)
		}

		if _, err := db.ExecContext(ctx, "DELETE FROM users WHERE id = $1", id); err != nil {
			b.Fatalf("DELETE failed: %v", err)
		}
	}
}

func benchPgStmt(b *testing.B, db *sql.DB) {
	ctx := context.Background()

	insertStmt, err := db.PrepareContext(ctx, "INSERT INTO users (name) VALUES ($1) RETURNING id")
	if err != nil {
		b.Fatalf("Prepare INSERT failed: %v", err)
	}
	defer insertStmt.Close()

	selectStmt, err := db.PrepareContext(ctx, "SELECT name FROM users WHERE id = $1")
	if err != nil {
		b.Fatalf("Prepare SELECT failed: %v", err)
	}
	defer selectStmt.Close()

	deleteStmt, err := db.PrepareContext(ctx, "DELETE FROM users WHERE id = $1")
	if err != nil {
		b.Fatalf("Prepare DELETE failed: %v", err)
	}
	defer deleteStmt.Close()

	b.ResetTimer()
	for b.Loop() {
		var id int64
		if err := insertStmt.QueryRowContext(ctx, "bench").Scan(&id); err != nil {
			b.Fatalf("stmt INSERT failed: %v", err)
		}

		var name string
		if err := selectStmt.QueryRowContext(ctx, id).Scan(&name); err != nil {
			b.Fatalf("stmt SELECT failed: %v", err)
		}

		if _, err := deleteStmt.ExecContext(ctx, id); err != nil {
			b.Fatalf("stmt DELETE failed: %v", err)
		}
	}
}

func BenchmarkPostgreSQL_RawDriver_ExecQuery(b *testing.B) {
	benchPgExecQuery(b, rawPgDB)
}

func BenchmarkPostgreSQL_CustomDriver_ExecQuery(b *testing.B) {
	benchPgExecQuery(b, testPgDB)
}

func BenchmarkPostgreSQL_RawDriver_Stmt(b *testing.B) {
	benchPgStmt(b, rawPgDB)
}

func BenchmarkPostgreSQL_CustomDriver_Stmt(b *testing.B) {
	benchPgStmt(b, testPgDB)
}

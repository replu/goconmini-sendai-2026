package customdriver

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

var (
	testMySQLDB *sql.DB
	rawMySQLDB  *sql.DB

	testPgDB *sql.DB
	rawPgDB  *sql.DB
)

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		slog.Error("Could not construct pool", "error", err)
		os.Exit(1)
	}

	if err := pool.Client.Ping(); err != nil {
		slog.Error("Could not connect to Docker", "error", err)
		os.Exit(1)
	}

	pool.MaxWait = 60 * time.Second

	// --- Start MySQL container ---
	mysqlResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mysql",
		Tag:        "8.0",
		Env: []string{
			"MYSQL_ROOT_PASSWORD=password",
			"MYSQL_DATABASE=testdb",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		slog.Error("Could not start MySQL", "error", err)
		os.Exit(1)
	}
	mysqlResource.Expire(180)

	// --- Start PostgreSQL container ---
	pgResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "18",
		Env: []string{
			"POSTGRES_USER=testuser",
			"POSTGRES_PASSWORD=password",
			"POSTGRES_DB=testdb",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		slog.Error("Could not start PostgreSQL", "error", err)
		_ = pool.Purge(mysqlResource)
		os.Exit(1)
	}
	pgResource.Expire(180)

	// --- Wait for MySQL ---
	mysqlHostPort := mysqlResource.GetHostPort("3306/tcp")
	mysqlDSN := fmt.Sprintf("root:password@tcp(%s)/testdb?parseTime=true", mysqlHostPort)

	if err := pool.Retry(func() error {
		tmpDB, retryErr := sql.Open("mysql", mysqlDSN)
		if retryErr != nil {
			return retryErr
		}
		defer tmpDB.Close()
		return tmpDB.Ping()
	}); err != nil {
		slog.Error("Could not connect to MySQL", "error", err)
		_ = pool.Purge(mysqlResource)
		_ = pool.Purge(pgResource)
		os.Exit(1)
	}

	// --- Wait for PostgreSQL ---
	pgDSN := fmt.Sprintf("host=localhost port=%s user=testuser password=password dbname=testdb sslmode=disable", pgResource.GetPort("5432/tcp"))

	if err := pool.Retry(func() error {
		tmpDB, retryErr := sql.Open("postgres", pgDSN)
		if retryErr != nil {
			return retryErr
		}
		defer tmpDB.Close()
		return tmpDB.Ping()
	}); err != nil {
		slog.Error("Could not connect to PostgreSQL", "error", err)
		_ = pool.Purge(mysqlResource)
		_ = pool.Purge(pgResource)
		os.Exit(1)
	}

	// --- Setup MySQL tables ---
	setupMySQLDB, err := sql.Open("mysql", mysqlDSN+"&multiStatements=true")
	if err != nil {
		slog.Error("Could not open MySQL setup connection", "error", err)
		_ = pool.Purge(mysqlResource)
		_ = pool.Purge(pgResource)
		os.Exit(1)
	}

	mysqlCreateTable := `CREATE TABLE IF NOT EXISTS users (
		id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		name varchar(128) NOT NULL,
		created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
		updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`

	if _, err := setupMySQLDB.Exec(mysqlCreateTable); err != nil {
		slog.Error("Could not create MySQL table", "error", err)
		setupMySQLDB.Close()
		_ = pool.Purge(mysqlResource)
		_ = pool.Purge(pgResource)
		os.Exit(1)
	}
	setupMySQLDB.Close()

	// --- Setup PostgreSQL tables ---
	setupPgDB, err := sql.Open("postgres", pgDSN)
	if err != nil {
		slog.Error("Could not open PostgreSQL setup connection", "error", err)
		_ = pool.Purge(mysqlResource)
		_ = pool.Purge(pgResource)
		os.Exit(1)
	}

	pgCreateTable := `CREATE TABLE IF NOT EXISTS users (
		id bigserial PRIMARY KEY,
		name varchar(128) NOT NULL,
		created_at timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP
	);`

	if _, err := setupPgDB.Exec(pgCreateTable); err != nil {
		slog.Error("Could not create PostgreSQL table", "error", err)
		setupPgDB.Close()
		_ = pool.Purge(mysqlResource)
		_ = pool.Purge(pgResource)
		os.Exit(1)
	}
	setupPgDB.Close()

	silentLogger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	// MySQL: CustomConnector-based
	mysqlConnector, err := mysql.MySQLDriver{}.OpenConnector(mysqlDSN)
	if err != nil {
		slog.Error("Could not create MySQL connector", "error", err)
		_ = pool.Purge(mysqlResource)
		_ = pool.Purge(pgResource)
		os.Exit(1)
	}
	testMySQLDB = sql.OpenDB(NewCustomConnector(mysqlConnector, silentLogger))

	// MySQL: raw driver
	rawMySQLDB, err = sql.Open("mysql", mysqlDSN)
	if err != nil {
		slog.Error("Could not open raw MySQL connection", "error", err)
		_ = pool.Purge(mysqlResource)
		_ = pool.Purge(pgResource)
		os.Exit(1)
	}

	// PostgreSQL: CustomConnector-based
	pgConnector, err := pq.NewConnector(pgDSN)
	if err != nil {
		slog.Error("Could not create PostgreSQL connector", "error", err)
		_ = pool.Purge(mysqlResource)
		_ = pool.Purge(pgResource)
		os.Exit(1)
	}
	testPgDB = sql.OpenDB(NewCustomConnector(pgConnector, silentLogger))

	// PostgreSQL: raw driver
	rawPgDB, err = sql.Open("postgres", pgDSN)
	if err != nil {
		slog.Error("Could not open raw PostgreSQL connection", "error", err)
		_ = pool.Purge(mysqlResource)
		_ = pool.Purge(pgResource)
		os.Exit(1)
	}

	code := m.Run()

	testMySQLDB.Close()
	rawMySQLDB.Close()
	testPgDB.Close()
	rawPgDB.Close()
	_ = pool.Purge(mysqlResource)
	_ = pool.Purge(pgResource)

	os.Exit(code)
}

// =============================================================================
// Helper
// =============================================================================

func truncateMySQLUsers(t *testing.T) {
	t.Helper()
	if _, err := testMySQLDB.Exec("TRUNCATE TABLE users"); err != nil {
		t.Fatalf("failed to truncate MySQL users table: %v", err)
	}
}

func truncatePgUsers(t *testing.T) {
	t.Helper()
	if _, err := testPgDB.Exec("TRUNCATE TABLE users RESTART IDENTITY"); err != nil {
		t.Fatalf("failed to truncate PostgreSQL users table: %v", err)
	}
}

// =============================================================================
// MySQL CRUD Tests
// =============================================================================

func TestMySQL_CustomDriverCRUD(t *testing.T) {
	t.Run("DirectConn_WithoutContext", testMySQLDirectConnWithoutContext)
	t.Run("DirectConn_WithContext", testMySQLDirectConnWithContext)
	t.Run("Stmt_WithoutContext", testMySQLStmtWithoutContext)
	t.Run("Stmt_WithContext", testMySQLStmtWithContext)
}

func testMySQLDirectConnWithoutContext(t *testing.T) {
	truncateMySQLUsers(t)

	// --- CREATE ---
	result, err := testMySQLDB.Exec("INSERT INTO users (name) VALUES (?)", "alice")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("LastInsertId failed: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive id, got %d", id)
	}

	// --- READ ---
	var name string
	var createdAt, updatedAt time.Time
	err = testMySQLDB.QueryRow(
		"SELECT name, created_at, updated_at FROM users WHERE id = ?", id,
	).Scan(&name, &createdAt, &updatedAt)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if name != "alice" {
		t.Errorf("expected name 'alice', got %q", name)
	}

	// --- UPDATE ---
	result, err = testMySQLDB.Exec("UPDATE users SET name = ? WHERE id = ?", "alice-updated", id)
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify update
	err = testMySQLDB.QueryRow(
		"SELECT name FROM users WHERE id = ?", id,
	).Scan(&name)
	if err != nil {
		t.Fatalf("SELECT after UPDATE failed: %v", err)
	}
	if name != "alice-updated" {
		t.Errorf("expected name 'alice-updated', got %q", name)
	}

	// --- DELETE ---
	result, err = testMySQLDB.Exec("DELETE FROM users WHERE id = ?", id)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	affected, err = result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify delete
	err = testMySQLDB.QueryRow(
		"SELECT name FROM users WHERE id = ?", id,
	).Scan(&name)
	if err != sql.ErrNoRows {
		t.Errorf("expected sql.ErrNoRows after DELETE, got %v", err)
	}
}

func testMySQLDirectConnWithContext(t *testing.T) {
	truncateMySQLUsers(t)
	ctx := context.Background()

	// --- CREATE ---
	result, err := testMySQLDB.ExecContext(ctx, "INSERT INTO users (name) VALUES (?)", "bob")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("LastInsertId failed: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive id, got %d", id)
	}

	// --- READ (single row) ---
	var name string
	var createdAt, updatedAt time.Time
	err = testMySQLDB.QueryRowContext(ctx,
		"SELECT name, created_at, updated_at FROM users WHERE id = ?", id,
	).Scan(&name, &createdAt, &updatedAt)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if name != "bob" {
		t.Errorf("expected name 'bob', got %q", name)
	}

	// --- READ (multiple rows) ---
	_, err = testMySQLDB.ExecContext(ctx, "INSERT INTO users (name) VALUES (?)", "bob2")
	if err != nil {
		t.Fatalf("INSERT second user failed: %v", err)
	}

	rows, err := testMySQLDB.QueryContext(ctx, "SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var rowID int64
		var rowName string
		if err := rows.Scan(&rowID, &rowName); err != nil {
			t.Fatalf("rows.Scan failed: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}

	// --- UPDATE ---
	result, err = testMySQLDB.ExecContext(ctx, "UPDATE users SET name = ? WHERE id = ?", "bob-updated", id)
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify update
	err = testMySQLDB.QueryRowContext(ctx,
		"SELECT name FROM users WHERE id = ?", id,
	).Scan(&name)
	if err != nil {
		t.Fatalf("SELECT after UPDATE failed: %v", err)
	}
	if name != "bob-updated" {
		t.Errorf("expected name 'bob-updated', got %q", name)
	}

	// --- DELETE ---
	result, err = testMySQLDB.ExecContext(ctx, "DELETE FROM users WHERE id = ?", id)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	affected, err = result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify delete
	err = testMySQLDB.QueryRowContext(ctx,
		"SELECT name FROM users WHERE id = ?", id,
	).Scan(&name)
	if err != sql.ErrNoRows {
		t.Errorf("expected sql.ErrNoRows after DELETE, got %v", err)
	}
}

func testMySQLStmtWithoutContext(t *testing.T) {
	truncateMySQLUsers(t)

	// --- CREATE ---
	insertStmt, err := testMySQLDB.Prepare("INSERT INTO users (name) VALUES (?)")
	if err != nil {
		t.Fatalf("Prepare INSERT failed: %v", err)
	}
	defer insertStmt.Close()

	result, err := insertStmt.Exec("charlie")
	if err != nil {
		t.Fatalf("stmt.Exec INSERT failed: %v", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("LastInsertId failed: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive id, got %d", id)
	}

	// --- READ ---
	selectStmt, err := testMySQLDB.Prepare(
		"SELECT name, created_at, updated_at FROM users WHERE id = ?",
	)
	if err != nil {
		t.Fatalf("Prepare SELECT failed: %v", err)
	}
	defer selectStmt.Close()

	var name string
	var createdAt, updatedAt time.Time
	err = selectStmt.QueryRow(id).Scan(&name, &createdAt, &updatedAt)
	if err != nil {
		t.Fatalf("stmt.QueryRow failed: %v", err)
	}
	if name != "charlie" {
		t.Errorf("expected name 'charlie', got %q", name)
	}

	// --- UPDATE ---
	updateStmt, err := testMySQLDB.Prepare("UPDATE users SET name = ? WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare UPDATE failed: %v", err)
	}
	defer updateStmt.Close()

	result, err = updateStmt.Exec("charlie-updated", id)
	if err != nil {
		t.Fatalf("stmt.Exec UPDATE failed: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify update
	err = selectStmt.QueryRow(id).Scan(&name, &createdAt, &updatedAt)
	if err != nil {
		t.Fatalf("stmt.QueryRow after UPDATE failed: %v", err)
	}
	if name != "charlie-updated" {
		t.Errorf("expected name 'charlie-updated', got %q", name)
	}

	// --- DELETE ---
	deleteStmt, err := testMySQLDB.Prepare("DELETE FROM users WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare DELETE failed: %v", err)
	}
	defer deleteStmt.Close()

	result, err = deleteStmt.Exec(id)
	if err != nil {
		t.Fatalf("stmt.Exec DELETE failed: %v", err)
	}
	affected, err = result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify delete
	err = selectStmt.QueryRow(id).Scan(&name, &createdAt, &updatedAt)
	if err != sql.ErrNoRows {
		t.Errorf("expected sql.ErrNoRows after DELETE, got %v", err)
	}
}

func testMySQLStmtWithContext(t *testing.T) {
	truncateMySQLUsers(t)
	ctx := context.Background()

	// --- CREATE ---
	insertStmt, err := testMySQLDB.PrepareContext(ctx, "INSERT INTO users (name) VALUES (?)")
	if err != nil {
		t.Fatalf("PrepareContext INSERT failed: %v", err)
	}
	defer insertStmt.Close()

	result, err := insertStmt.ExecContext(ctx, "dave")
	if err != nil {
		t.Fatalf("stmt.ExecContext INSERT failed: %v", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("LastInsertId failed: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive id, got %d", id)
	}

	// --- READ (single row) ---
	selectStmt, err := testMySQLDB.PrepareContext(ctx,
		"SELECT name, created_at, updated_at FROM users WHERE id = ?",
	)
	if err != nil {
		t.Fatalf("PrepareContext SELECT failed: %v", err)
	}
	defer selectStmt.Close()

	var name string
	var createdAt, updatedAt time.Time
	err = selectStmt.QueryRowContext(ctx, id).Scan(&name, &createdAt, &updatedAt)
	if err != nil {
		t.Fatalf("stmt.QueryRowContext failed: %v", err)
	}
	if name != "dave" {
		t.Errorf("expected name 'dave', got %q", name)
	}

	// --- READ (multiple rows) ---
	_, err = insertStmt.ExecContext(ctx, "dave2")
	if err != nil {
		t.Fatalf("stmt.ExecContext second INSERT failed: %v", err)
	}

	listStmt, err := testMySQLDB.PrepareContext(ctx, "SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("PrepareContext list SELECT failed: %v", err)
	}
	defer listStmt.Close()

	rows, err := listStmt.QueryContext(ctx)
	if err != nil {
		t.Fatalf("stmt.QueryContext failed: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var rowID int64
		var rowName string
		if err := rows.Scan(&rowID, &rowName); err != nil {
			t.Fatalf("rows.Scan failed: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}

	// --- UPDATE ---
	updateStmt, err := testMySQLDB.PrepareContext(ctx, "UPDATE users SET name = ? WHERE id = ?")
	if err != nil {
		t.Fatalf("PrepareContext UPDATE failed: %v", err)
	}
	defer updateStmt.Close()

	result, err = updateStmt.ExecContext(ctx, "dave-updated", id)
	if err != nil {
		t.Fatalf("stmt.ExecContext UPDATE failed: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify update
	err = selectStmt.QueryRowContext(ctx, id).Scan(&name, &createdAt, &updatedAt)
	if err != nil {
		t.Fatalf("stmt.QueryRowContext after UPDATE failed: %v", err)
	}
	if name != "dave-updated" {
		t.Errorf("expected name 'dave-updated', got %q", name)
	}

	// --- DELETE ---
	deleteStmt, err := testMySQLDB.PrepareContext(ctx, "DELETE FROM users WHERE id = ?")
	if err != nil {
		t.Fatalf("PrepareContext DELETE failed: %v", err)
	}
	defer deleteStmt.Close()

	result, err = deleteStmt.ExecContext(ctx, id)
	if err != nil {
		t.Fatalf("stmt.ExecContext DELETE failed: %v", err)
	}
	affected, err = result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify delete
	err = selectStmt.QueryRowContext(ctx, id).Scan(&name, &createdAt, &updatedAt)
	if err != sql.ErrNoRows {
		t.Errorf("expected sql.ErrNoRows after DELETE, got %v", err)
	}
}

// =============================================================================
// PostgreSQL CRUD Tests
// =============================================================================

func TestPostgreSQL_CustomDriverCRUD(t *testing.T) {
	t.Run("DirectConn_WithoutContext", testPgDirectConnWithoutContext)
	t.Run("DirectConn_WithContext", testPgDirectConnWithContext)
	t.Run("Stmt_WithoutContext", testPgStmtWithoutContext)
	t.Run("Stmt_WithContext", testPgStmtWithContext)
}

func testPgDirectConnWithoutContext(t *testing.T) {
	truncatePgUsers(t)

	// --- CREATE ---
	var id int64
	err := testPgDB.QueryRow("INSERT INTO users (name) VALUES ($1) RETURNING id", "alice").Scan(&id)
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive id, got %d", id)
	}

	// --- READ ---
	var name string
	var createdAt, updatedAt time.Time
	err = testPgDB.QueryRow(
		"SELECT name, created_at, updated_at FROM users WHERE id = $1", id,
	).Scan(&name, &createdAt, &updatedAt)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if name != "alice" {
		t.Errorf("expected name 'alice', got %q", name)
	}

	// --- UPDATE ---
	result, err := testPgDB.Exec("UPDATE users SET name = $1 WHERE id = $2", "alice-updated", id)
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify update
	err = testPgDB.QueryRow(
		"SELECT name FROM users WHERE id = $1", id,
	).Scan(&name)
	if err != nil {
		t.Fatalf("SELECT after UPDATE failed: %v", err)
	}
	if name != "alice-updated" {
		t.Errorf("expected name 'alice-updated', got %q", name)
	}

	// --- DELETE ---
	result, err = testPgDB.Exec("DELETE FROM users WHERE id = $1", id)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	affected, err = result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify delete
	err = testPgDB.QueryRow(
		"SELECT name FROM users WHERE id = $1", id,
	).Scan(&name)
	if err != sql.ErrNoRows {
		t.Errorf("expected sql.ErrNoRows after DELETE, got %v", err)
	}
}

func testPgDirectConnWithContext(t *testing.T) {
	truncatePgUsers(t)
	ctx := context.Background()

	// --- CREATE ---
	var id int64
	err := testPgDB.QueryRowContext(ctx, "INSERT INTO users (name) VALUES ($1) RETURNING id", "bob").Scan(&id)
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive id, got %d", id)
	}

	// --- READ (single row) ---
	var name string
	var createdAt, updatedAt time.Time
	err = testPgDB.QueryRowContext(ctx,
		"SELECT name, created_at, updated_at FROM users WHERE id = $1", id,
	).Scan(&name, &createdAt, &updatedAt)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if name != "bob" {
		t.Errorf("expected name 'bob', got %q", name)
	}

	// --- READ (multiple rows) ---
	_, err = testPgDB.ExecContext(ctx, "INSERT INTO users (name) VALUES ($1)", "bob2")
	if err != nil {
		t.Fatalf("INSERT second user failed: %v", err)
	}

	rows, err := testPgDB.QueryContext(ctx, "SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var rowID int64
		var rowName string
		if err := rows.Scan(&rowID, &rowName); err != nil {
			t.Fatalf("rows.Scan failed: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}

	// --- UPDATE ---
	result, err := testPgDB.ExecContext(ctx, "UPDATE users SET name = $1 WHERE id = $2", "bob-updated", id)
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify update
	err = testPgDB.QueryRowContext(ctx,
		"SELECT name FROM users WHERE id = $1", id,
	).Scan(&name)
	if err != nil {
		t.Fatalf("SELECT after UPDATE failed: %v", err)
	}
	if name != "bob-updated" {
		t.Errorf("expected name 'bob-updated', got %q", name)
	}

	// --- DELETE ---
	result, err = testPgDB.ExecContext(ctx, "DELETE FROM users WHERE id = $1", id)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	affected, err = result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify delete
	err = testPgDB.QueryRowContext(ctx,
		"SELECT name FROM users WHERE id = $1", id,
	).Scan(&name)
	if err != sql.ErrNoRows {
		t.Errorf("expected sql.ErrNoRows after DELETE, got %v", err)
	}
}

func testPgStmtWithoutContext(t *testing.T) {
	truncatePgUsers(t)

	// --- CREATE ---
	insertStmt, err := testPgDB.Prepare("INSERT INTO users (name) VALUES ($1) RETURNING id")
	if err != nil {
		t.Fatalf("Prepare INSERT failed: %v", err)
	}
	defer insertStmt.Close()

	var id int64
	err = insertStmt.QueryRow("charlie").Scan(&id)
	if err != nil {
		t.Fatalf("stmt.QueryRow INSERT failed: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive id, got %d", id)
	}

	// --- READ ---
	selectStmt, err := testPgDB.Prepare(
		"SELECT name, created_at, updated_at FROM users WHERE id = $1",
	)
	if err != nil {
		t.Fatalf("Prepare SELECT failed: %v", err)
	}
	defer selectStmt.Close()

	var name string
	var createdAt, updatedAt time.Time
	err = selectStmt.QueryRow(id).Scan(&name, &createdAt, &updatedAt)
	if err != nil {
		t.Fatalf("stmt.QueryRow failed: %v", err)
	}
	if name != "charlie" {
		t.Errorf("expected name 'charlie', got %q", name)
	}

	// --- UPDATE ---
	updateStmt, err := testPgDB.Prepare("UPDATE users SET name = $1 WHERE id = $2")
	if err != nil {
		t.Fatalf("Prepare UPDATE failed: %v", err)
	}
	defer updateStmt.Close()

	result, err := updateStmt.Exec("charlie-updated", id)
	if err != nil {
		t.Fatalf("stmt.Exec UPDATE failed: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify update
	err = selectStmt.QueryRow(id).Scan(&name, &createdAt, &updatedAt)
	if err != nil {
		t.Fatalf("stmt.QueryRow after UPDATE failed: %v", err)
	}
	if name != "charlie-updated" {
		t.Errorf("expected name 'charlie-updated', got %q", name)
	}

	// --- DELETE ---
	deleteStmt, err := testPgDB.Prepare("DELETE FROM users WHERE id = $1")
	if err != nil {
		t.Fatalf("Prepare DELETE failed: %v", err)
	}
	defer deleteStmt.Close()

	result, err = deleteStmt.Exec(id)
	if err != nil {
		t.Fatalf("stmt.Exec DELETE failed: %v", err)
	}
	affected, err = result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify delete
	err = selectStmt.QueryRow(id).Scan(&name, &createdAt, &updatedAt)
	if err != sql.ErrNoRows {
		t.Errorf("expected sql.ErrNoRows after DELETE, got %v", err)
	}
}

func testPgStmtWithContext(t *testing.T) {
	truncatePgUsers(t)
	ctx := context.Background()

	// --- CREATE ---
	insertStmt, err := testPgDB.PrepareContext(ctx, "INSERT INTO users (name) VALUES ($1) RETURNING id")
	if err != nil {
		t.Fatalf("PrepareContext INSERT failed: %v", err)
	}
	defer insertStmt.Close()

	var id int64
	err = insertStmt.QueryRowContext(ctx, "dave").Scan(&id)
	if err != nil {
		t.Fatalf("stmt.QueryRowContext INSERT failed: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive id, got %d", id)
	}

	// --- READ (single row) ---
	selectStmt, err := testPgDB.PrepareContext(ctx,
		"SELECT name, created_at, updated_at FROM users WHERE id = $1",
	)
	if err != nil {
		t.Fatalf("PrepareContext SELECT failed: %v", err)
	}
	defer selectStmt.Close()

	var name string
	var createdAt, updatedAt time.Time
	err = selectStmt.QueryRowContext(ctx, id).Scan(&name, &createdAt, &updatedAt)
	if err != nil {
		t.Fatalf("stmt.QueryRowContext failed: %v", err)
	}
	if name != "dave" {
		t.Errorf("expected name 'dave', got %q", name)
	}

	// --- READ (multiple rows) ---
	_, err = insertStmt.ExecContext(ctx, "dave2")
	if err != nil {
		t.Fatalf("stmt.ExecContext second INSERT failed: %v", err)
	}

	listStmt, err := testPgDB.PrepareContext(ctx, "SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("PrepareContext list SELECT failed: %v", err)
	}
	defer listStmt.Close()

	rows, err := listStmt.QueryContext(ctx)
	if err != nil {
		t.Fatalf("stmt.QueryContext failed: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var rowID int64
		var rowName string
		if err := rows.Scan(&rowID, &rowName); err != nil {
			t.Fatalf("rows.Scan failed: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}

	// --- UPDATE ---
	updateStmt, err := testPgDB.PrepareContext(ctx, "UPDATE users SET name = $1 WHERE id = $2")
	if err != nil {
		t.Fatalf("PrepareContext UPDATE failed: %v", err)
	}
	defer updateStmt.Close()

	result, err := updateStmt.ExecContext(ctx, "dave-updated", id)
	if err != nil {
		t.Fatalf("stmt.ExecContext UPDATE failed: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify update
	err = selectStmt.QueryRowContext(ctx, id).Scan(&name, &createdAt, &updatedAt)
	if err != nil {
		t.Fatalf("stmt.QueryRowContext after UPDATE failed: %v", err)
	}
	if name != "dave-updated" {
		t.Errorf("expected name 'dave-updated', got %q", name)
	}

	// --- DELETE ---
	deleteStmt, err := testPgDB.PrepareContext(ctx, "DELETE FROM users WHERE id = $1")
	if err != nil {
		t.Fatalf("PrepareContext DELETE failed: %v", err)
	}
	defer deleteStmt.Close()

	result, err = deleteStmt.ExecContext(ctx, id)
	if err != nil {
		t.Fatalf("stmt.ExecContext DELETE failed: %v", err)
	}
	affected, err = result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row affected, got %d", affected)
	}

	// Verify delete
	err = selectStmt.QueryRowContext(ctx, id).Scan(&name, &createdAt, &updatedAt)
	if err != sql.ErrNoRows {
		t.Errorf("expected sql.ErrNoRows after DELETE, got %v", err)
	}
}

// =============================================================================
// Transaction Tests
// =============================================================================

func TestMySQL_Transaction(t *testing.T) {
	truncateMySQLUsers(t)
	ctx := context.Background()

	t.Run("Commit", func(t *testing.T) {
		truncateMySQLUsers(t)

		tx, err := testMySQLDB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx failed: %v", err)
		}

		_, err = tx.ExecContext(ctx, "INSERT INTO users (name) VALUES (?)", "tx-user")
		if err != nil {
			tx.Rollback()
			t.Fatalf("INSERT in tx failed: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		var name string
		err = testMySQLDB.QueryRowContext(ctx, "SELECT name FROM users WHERE name = ?", "tx-user").Scan(&name)
		if err != nil {
			t.Fatalf("SELECT after commit failed: %v", err)
		}
		if name != "tx-user" {
			t.Errorf("expected name 'tx-user', got %q", name)
		}
	})

	t.Run("Rollback", func(t *testing.T) {
		truncateMySQLUsers(t)

		tx, err := testMySQLDB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx failed: %v", err)
		}

		_, err = tx.ExecContext(ctx, "INSERT INTO users (name) VALUES (?)", "rollback-user")
		if err != nil {
			tx.Rollback()
			t.Fatalf("INSERT in tx failed: %v", err)
		}

		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback failed: %v", err)
		}

		err = testMySQLDB.QueryRowContext(ctx, "SELECT name FROM users WHERE name = ?", "rollback-user").Scan(new(string))
		if err != sql.ErrNoRows {
			t.Errorf("expected sql.ErrNoRows after rollback, got %v", err)
		}
	})
}

func TestPostgreSQL_Transaction(t *testing.T) {
	truncatePgUsers(t)
	ctx := context.Background()

	t.Run("Commit", func(t *testing.T) {
		truncatePgUsers(t)

		tx, err := testPgDB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx failed: %v", err)
		}

		_, err = tx.ExecContext(ctx, "INSERT INTO users (name) VALUES ($1)", "tx-user")
		if err != nil {
			tx.Rollback()
			t.Fatalf("INSERT in tx failed: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		var name string
		err = testPgDB.QueryRowContext(ctx, "SELECT name FROM users WHERE name = $1", "tx-user").Scan(&name)
		if err != nil {
			t.Fatalf("SELECT after commit failed: %v", err)
		}
		if name != "tx-user" {
			t.Errorf("expected name 'tx-user', got %q", name)
		}
	})

	t.Run("Rollback", func(t *testing.T) {
		truncatePgUsers(t)

		tx, err := testPgDB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx failed: %v", err)
		}

		_, err = tx.ExecContext(ctx, "INSERT INTO users (name) VALUES ($1)", "rollback-user")
		if err != nil {
			tx.Rollback()
			t.Fatalf("INSERT in tx failed: %v", err)
		}

		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback failed: %v", err)
		}

		err = testPgDB.QueryRowContext(ctx, "SELECT name FROM users WHERE name = $1", "rollback-user").Scan(new(string))
		if err != sql.ErrNoRows {
			t.Errorf("expected sql.ErrNoRows after rollback, got %v", err)
		}
	})
}

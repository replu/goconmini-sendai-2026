-- name: GetUserByName :one
SELECT * FROM users
WHERE name = $1 LIMIT 1;

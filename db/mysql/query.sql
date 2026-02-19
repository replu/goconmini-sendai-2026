-- name: GetUserByName :one
SELECT * FROM users
WHERE name = ? LIMIT 1;

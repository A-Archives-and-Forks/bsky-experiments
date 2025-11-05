-- name: CreateLike :exec
INSERT INTO likes (actor_uid, rkey, subj, created_at)
VALUES (
        sqlc.arg('actor_uid'),
        sqlc.arg('rkey'),
        sqlc.arg('subj'),
        sqlc.arg('created_at')
    );
-- name: DeleteLike :exec
DELETE FROM likes
WHERE actor_uid = $1
    AND rkey = $2;
-- name: GetLike :one
SELECT l.*,
    s.actor_did AS subject_actor_did,
    c.name AS subject_namespace,
    s.rkey AS subject_rkey
FROM likes l
    JOIN subjects s ON l.subj = s.id
    JOIN collections c ON s.col = c.id
WHERE l.actor_uid = $1
    AND l.rkey = $2
LIMIT 1;
-- name: TrimLikes :execrows
DELETE FROM likes
WHERE created_at < NOW() - make_interval(hours := $1)
    OR created_at > NOW() + make_interval(mins := 15);
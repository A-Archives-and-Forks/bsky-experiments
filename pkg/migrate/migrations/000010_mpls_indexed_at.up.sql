DROP VIEW IF EXISTS mpls;

CREATE MATERIALIZED VIEW IF NOT EXISTS mpls
ENGINE = ReplacingMergeTree()
ORDER BY (indexed_at, rkey)
TTL indexed_at + INTERVAL 72 HOUR
POPULATE AS
SELECT
    p.did AS actor_did,
    p.rkey AS rkey,
    p.created_at AS created_at,
    p.indexed_at AS indexed_at
FROM posts AS p
INNER JOIN (
    SELECT DISTINCT actor_did
    FROM actor_labels
    WHERE label = 'mpls' AND deleted = 0
) AS labeled_actors ON p.did = labeled_actors.actor_did
WHERE p.deleted = 0
    AND p.parent_uri = ''
    AND p.root_uri = '';

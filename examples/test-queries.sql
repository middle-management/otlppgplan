-- Sample queries to test auto_explain
-- These will generate EXPLAIN output that will be captured as traces

-- Simple query with index scan
SELECT * FROM users WHERE email = 'user100@example.com';

-- Join query
SELECT u.name, u.email, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.created_at > NOW() - INTERVAL '7 days'
GROUP BY u.id, u.name, u.email
HAVING COUNT(o.id) > 3;

-- Complex query with multiple joins and aggregations
SELECT
    u.name,
    o.status,
    COUNT(*) as count,
    AVG(o.total) as avg_total,
    SUM(o.total) as total_spent
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.created_at > NOW() - INTERVAL '30 days'
GROUP BY u.name, o.status
ORDER BY total_spent DESC
LIMIT 10;

-- Query using the view
SELECT * FROM user_order_summary
WHERE order_count > 5
ORDER BY total_spent DESC
LIMIT 20;

-- Subquery example
SELECT u.*
FROM users u
WHERE u.id IN (
    SELECT user_id
    FROM orders
    WHERE status = 'completed'
    AND total > 500
    GROUP BY user_id
    HAVING COUNT(*) > 3
);

-- CTE example
WITH recent_orders AS (
    SELECT user_id, COUNT(*) as order_count, SUM(total) as total_spent
    FROM orders
    WHERE created_at > NOW() - INTERVAL '7 days'
    GROUP BY user_id
)
SELECT u.name, u.email, ro.order_count, ro.total_spent
FROM users u
JOIN recent_orders ro ON u.id = ro.user_id
WHERE ro.total_spent > 1000
ORDER BY ro.total_spent DESC;

SELECT c.region, count(*) orders, sum(o.amount_cents) revenue_cents
FROM input_orders o JOIN input_customers c ON o.customer_id = c.customer_id
GROUP BY c.region

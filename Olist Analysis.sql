/*1. CUSTOMER ANALYSIS*/

----------------------------------------------------------------------------------
/* a. Customer Segmentation: Segment customers based on their purchase behavior */
----------------------------------------------------------------------------------

-- a.1. Frequency of purchases
SELECT 
    c.customer_unique_id, 
    COUNT(op.order_id) AS purchases_frequency
FROM order_payments op
JOIN orders o ON op.order_id = o.order_id
JOIN customers c ON c.customer_id = o.customer_id
GROUP BY c.customer_unique_id 
ORDER BY purchases_frequency DESC;

-- a.2. Average order value 

SELECT 
    oi.order_id, 
    SUM(oi.price + oi.freight_value) AS order_total_value, 
    COUNT(oi.order_id) AS number_of_orders, 
    (SUM(oi.price + oi.freight_value) / COUNT(oi.order_id)) AS aov
FROM orders o 
JOIN order_items oi ON o.order_id = oi.order_id 
GROUP BY oi.order_id;
-------------------------------------------------------------------------------------------
/* b. Repeat Customer Rate: Percentage of customers who have made more than one purchase */
-------------------------------------------------------------------------------------------
WITH customers_purchase AS ( 
    SELECT 
        c.customer_unique_id,
        COUNT(c.customer_unique_id) AS repeat_customer_number
    FROM orders o 
    JOIN customers c ON o.customer_id = c.customer_id 
    JOIN order_items oi ON o.order_id = oi.order_id 
    GROUP BY c.customer_unique_id 
    HAVING COUNT(oi.order_id) > 1 
)
SELECT 
    SUM(repeat_customer_number) / (
        SELECT COUNT(c.customer_unique_id)
        FROM orders o 
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN order_items oi ON oi.order_id = o.order_id
    ) AS repeat_customer_ratio
FROM customers_purchase;
---------------------------------
/* c. Customer Lifetime Value */
---------------------------------
WITH CLV AS (
    SELECT
        c.customer_state, 
        COUNT(DISTINCT o.order_id) AS number_of_orders,
        SUM(op.payment_value) AS total_spent,
        AVG(op.payment_value) AS avg_order_value,
        DATE_PART('day', MAX(CAST(o.order_purchase_timestamp AS TIMESTAMP)) - MIN(CAST(o.order_purchase_timestamp AS TIMESTAMP))) AS customer_lifetime_days,
        (SUM(op.payment_value) / COUNT(DISTINCT o.order_id)) * (DATE_PART('day', MAX(CAST(o.order_purchase_timestamp AS TIMESTAMP)) - MIN(CAST(o.order_purchase_timestamp AS TIMESTAMP))) / 30.0) AS customer_lifetime_value  
    FROM orders o 
    JOIN order_payments op ON o.order_id = op.order_id 
    JOIN customers c ON c.customer_id = o.customer_id 
    GROUP BY c.customer_state 
    ORDER BY customer_lifetime_value DESC
)
SELECT 
    AVG(customer_lifetime_value)
FROM CLV;
----------------------------------------------
/* d. Customer Segmentation (RFM Analysis) */
----------------------------------------------
WITH rfm_table AS (
    -- CTE: calculate RFM
    SELECT 
        distinct c.customer_id,
        MAX(CAST(o.order_purchase_timestamp AS TIMESTAMP)) AS last_purchase_date,
        COUNT(DISTINCT o.order_id) AS num_orders,
        SUM(op.payment_value) AS total_spent
    FROM orders o 
    JOIN order_payments op ON o.order_id = op.order_id 
    JOIN customers c ON c.customer_id = o.customer_id 
    GROUP BY distinct c.customer_id 
),
rfm_calc AS (
    SELECT 
        distinct customer_id,
        rfm_recency,
        rfm_frequency,
        rfm_monetary_value,
        CAST(rfm_recency AS TEXT) || CAST(rfm_frequency AS TEXT) || CAST(rfm_monetary_value AS TEXT) AS rfm_cell
    FROM (
        SELECT 
            customer_id,
            NTILE(5) OVER (ORDER BY DATE_TRUNC('month', last_purchase_date)) AS rfm_recency,
            NTILE(5) OVER (ORDER BY num_orders) AS rfm_frequency,
            NTILE(5) OVER (ORDER BY total_spent) AS rfm_monetary_value
        FROM rfm_table
    ) AS rfm_calc_table
)
SELECT 
    rfm_segment,
    COUNT(o.order_id) AS num_orders
FROM (
    SELECT 
        distinct customer_id,
        rfm_recency,
        rfm_frequency,
        rfm_monetary_value,
        rfm_cell,
        CASE 
            WHEN rfm_recency IN ('4','5') AND rfm_frequency IN ('4', '5') AND rfm_monetary_value IN ('4', '5') THEN 'Champions'
            WHEN rfm_recency BETWEEN '2' AND '5' AND rfm_frequency BETWEEN '3' AND '5' AND rfm_monetary_value BETWEEN '3' AND '5' THEN 'Loyal Customers'
            WHEN rfm_recency BETWEEN '3' AND '5' AND rfm_frequency BETWEEN '1' AND '3' AND rfm_monetary_value BETWEEN '1' AND '3' THEN 'Potential Loyalist'
            WHEN rfm_recency BETWEEN '4' AND '5' AND rfm_frequency BETWEEN '0' AND '1' AND rfm_monetary_value BETWEEN '0' AND '1' THEN 'Recent Customers'
            WHEN rfm_recency BETWEEN '3' AND '4' AND rfm_frequency BETWEEN '0' AND '1' AND rfm_monetary_value BETWEEN '0' AND '1' THEN 'Promising'
            WHEN rfm_recency BETWEEN '2' AND '3' AND rfm_frequency BETWEEN '2' AND '3' AND rfm_monetary_value BETWEEN '2' AND '3' THEN 'Customers Needing Attention'
            WHEN rfm_recency BETWEEN '2' AND '3' AND rfm_frequency BETWEEN '0' AND '2' AND rfm_monetary_value BETWEEN '0' AND '2' THEN 'About To Sleep'
            WHEN rfm_recency BETWEEN '0' AND '2' AND rfm_frequency BETWEEN '2' AND '5' AND rfm_monetary_value BETWEEN '2' AND '5' THEN 'At Risk'
            WHEN rfm_recency BETWEEN '0' AND '1' AND rfm_frequency BETWEEN '4' AND '5' AND rfm_monetary_value BETWEEN '4' AND '5' THEN 'Can’t Lose Them'
            WHEN rfm_recency BETWEEN '1' AND '2' AND rfm_frequency BETWEEN '1' AND '2' AND rfm_monetary_value BETWEEN '1' AND '2' THEN 'Hibernating'
            WHEN rfm_recency BETWEEN '0' AND '2' AND rfm_frequency BETWEEN '0' AND '2' AND rfm_monetary_value BETWEEN '0' AND '2' THEN 'Lost'
            ELSE 'Other' 
        END AS rfm_segment
    FROM rfm_calc
) AS rfm_segments
JOIN orders o ON rfm_segments.customer_id = o.customer_id
GROUP BY rfm_segment
ORDER BY num_orders DESC;

-------------------------------
/* e. Customer Satisfaction */
-------------------------------
SELECT 
    DATE_TRUNC('month', or2.review_creation_date::timestamp) AS month_reviewed,
    AVG(or2.review_score) AS avg_review_score,
    COUNT(*) AS num_responses
FROM orders o 
JOIN order_reviews or2 ON o.order_id = or2.order_id
WHERE or2.review_score IS NOT NULL 
GROUP BY DATE_TRUNC('month', or2.review_creation_date::timestamp)
ORDER BY month_reviewed;

/*2. Product and Sales Analysis */
--------------------------
-- a. Monthly Sales Trend
--------------------------
SELECT 
    DATE_TRUNC('month', CAST(o.order_purchase_timestamp AS TIMESTAMP)) AS month,
    SUM(op.payment_value) AS total_sales
FROM orders o 
JOIN order_payments op ON o.order_id = op.order_id 
GROUP BY DATE_TRUNC('month', CAST(o.order_purchase_timestamp AS TIMESTAMP))
ORDER BY month;
------------------------
-- b. Weekly Sales Trend
------------------------
SELECT 
    DATE_TRUNC('week', CAST(o.order_purchase_timestamp AS TIMESTAMP)) AS week,
    SUM(op.payment_value) AS total_sales
FROM orders o 
JOIN order_payments op ON o.order_id = op.order_id 
GROUP BY DATE_TRUNC('week', CAST(o.order_purchase_timestamp AS TIMESTAMP))
ORDER BY week;
-----------------------
-- c. Daily Sales Trend
-----------------------
SELECT 
    DATE_TRUNC('day', CAST(o.order_purchase_timestamp AS TIMESTAMP)) AS day,
    SUM(op.payment_value) AS total_sales
FROM orders o 
JOIN order_payments op ON o.order_id = op.order_id 
GROUP BY DATE_TRUNC('day', CAST(o.order_purchase_timestamp AS TIMESTAMP))
ORDER BY day;
--------------------------------
-- b. Product Sales Distribution
--------------------------------
SELECT 
    pcnt.product_category_name_english,
    SUM(op.payment_value) AS total_sales 
FROM order_items oi 
JOIN products p ON oi.product_id = p.product_id 
JOIN order_payments op ON oi.order_id = op.order_id 
JOIN product_category_name_translation pcnt ON pcnt.product_category_name = p.product_category_name 
GROUP BY pcnt.product_category_name_english 
ORDER BY total_sales DESC;
----------------------------------
-- c. Product Performance Analysis
----------------------------------
WITH category_sales AS (
    SELECT 
        pcnt.product_category_name_english AS category,
        SUM(oi.price) AS total_revenue,
        SUM(oi.order_item_id) AS total_quantity
    FROM order_items oi 
    JOIN products p ON oi.product_id = p.product_id 
    JOIN product_category_name_translation pcnt ON pcnt.product_category_name = p.product_category_name 	
    JOIN orders o ON o.order_id = oi.order_id 
    WHERE o.order_status = 'delivered'
    GROUP BY pcnt.product_category_name_english 
)
SELECT 
    category,
    total_revenue,
    total_quantity
FROM category_sales
ORDER BY total_revenue DESC, total_quantity DESC
LIMIT 10;

/* 3. Operational Efficiency */
--------------------------------
-- a. Order Fulfillment Analysis
--------------------------------

-- a.1. Total order processing time
SELECT 
    o.order_id, 
    o.customer_id,
    o.order_status,
    DATE_PART('day', CAST(o.order_delivered_customer_date AS TIMESTAMP) - CAST(o.order_purchase_timestamp AS TIMESTAMP)) AS total_processing_time_days
FROM orders o 
WHERE o.order_status = 'delivered'
ORDER BY total_processing_time_days DESC;

-- a.2. Specific stage of order processing
SELECT 
    o.order_id, 
    o.customer_id, 
    o.order_status,
    DATE_PART('day', CAST(o.order_delivered_customer_date AS TIMESTAMP) - CAST(o.order_approved_at AS TIMESTAMP)) AS approval_to_delivery_days
FROM orders o 
WHERE o.order_status = 'delivered'
ORDER BY approval_to_delivery_days DESC;
-------------------------------
-- b. Shipping Time Analysis 
-------------------------------

SELECT 
    o.order_id, 
    c.customer_state,
    DATE_PART('day', CAST(o.order_delivered_customer_date AS TIMESTAMP) - CAST(o.order_estimated_delivery_date AS TIMESTAMP)) AS estimated_delivery_time_days
FROM orders o 
JOIN customers c ON o.customer_id = c.customer_id 
WHERE o.order_status = 'delivered'
ORDER BY estimated_delivery_time_days DESC;

/*4. Payment Behavior*/
-----------------------------
--a.Payment method Analysis
-----------------------------
select 
	op.payment_type,
	count(*) as num_payments,
	sum(op.payment_value) as total_payment_value
from order_payments op 
group by op.payment_type 
order by total_payment_value desc 

--------------------------------
--b. AOV for each payment method
--------------------------------
select 	
	op.payment_type,
	avg(op.payment_value) as avg_order_value
from order_payments op 
group by op.payment_type 
order by avg_order_value desc 

-----------------------------------------------
--c. Impact of payment methods on AOV over time
-----------------------------------------------
select 
	op.payment_type,
	date_trunc('month', o.order_purchase_timestamp::timestamp) as month,
	avg(op.payment_value) as avg_order_value
from order_payments op 
join orders o 
on o.order_id = op.order_id 
where o.order_status = 'delivered'
group by op.payment_type, date_trunc('month', o.order_purchase_timestamp::timestamp)
order by month

/*5. Geolocation analysis*/

----------------------------------
--a. Geographic sales distribution
----------------------------------

select 
	g.geolocation_state as state,
	count(o.order_id) as num_orders,
	sum(oi.price) as total_sales
from orders o 
join order_items oi 
on o.order_id = oi.order_id 
join customers c 
on c.customer_id = o.customer_id 
join geolocation g 
on g.geolocation_zip_code_prefix = c.customer_zip_code_prefix 
group by g.geolocation_state 
order by total_sales desc 

-------------------------------
--b. Customer Density 
-------------------------------
select 
	g.geolocation_state as state,
	count(c.customer_unique_id) as num_customers,
	count(oi.order_id) as num_orders,
	sum(oi.price) as total_sales
from orders o 
join order_items oi 
on o.order_id = oi.order_id 
join customers c 
on c.customer_id = o.customer_id 
join geolocation g 
on g.geolocation_zip_code_prefix = c.customer_zip_code_prefix 
group by g.geolocation_state 
order by num_customers desc 

-----------------------------------
--c. Delivery performance by Region
-----------------------------------
select 
	g.geolocation_state as state,
	avg(date_part('day',cast(o.order_delivered_customer_date as timestamp) - cast(o.order_purchase_timestamp as timestamp))) as avg_delivery_time_days
from orders o 
join customers c 
on c.customer_id = o.customer_id 
join geolocation g 
on g.geolocation_zip_code_prefix = c.customer_zip_code_prefix 
where o.order_status = 'delivered'
group by g.geolocation_state 
order by avg_delivery_time_days asc

----------------------------------------------
--d. Product and category popularity by Region
----------------------------------------------
select 
	g.geolocation_state as state,
	pcnt.product_category_name_english as category,
	count(oi.order_id) as num_orders,
	sum(oi.price) as total_sales
from orders o 
join order_items oi 
on o.order_id = oi.order_id 
join products p 
on p.product_id = oi.product_id 
join customers c 
on c.customer_id = o.customer_id 
join geolocation g 
on g.geolocation_zip_code_prefix = c.customer_zip_code_prefix 
join product_category_name_translation pcnt 
on pcnt.product_category_name = p.product_category_name 
group by g.geolocation_state, pcnt.product_category_name_english 
order by state, total_sales desc 

--------------------------------------------------------
--e.How regional impact on sales an inventory management
--------------------------------------------------------
with regional_sales as (
	select 
		g.geolocation_state as state,
		pcnt.product_category_name_english as category,
		count(oi.order_id) as num_orders,
		sum(oi.price) as total_sales
	from orders o 
	join order_items oi 
	on o.order_id = oi.order_id 
	join products p 
	on p.product_id = oi.product_id 
	join customers c 
	on c.customer_id = o.customer_id 
	join geolocation g 
	on g.geolocation_zip_code_prefix = c.customer_zip_code_prefix 
	join product_category_name_translation pcnt 
	on pcnt.product_category_name = p.product_category_name
	group by g.geolocation_state, pcnt.product_category_name_english
)
select 
    state,
    category,
    num_orders,
    total_sales,
    round(cast(total_sales as numeric) * 1.0 / sum(cast(total_sales as numeric)) over (partition by state), 2) as sales_percentage,
    round(cast(num_orders as numeric) * 1.0 / sum(cast(num_orders as numeric)) over (partition by state), 2) as orders_percentage
from
    regional_sales
order by
    state,
    total_sales desc;
   
/*Ad-Hoc Analysis*/


----------------------------------
--Top 10 sales by product category
----------------------------------

select 
	pcnt.product_category_name_english,
	sum(op.payment_value) as total_sales 
from order_payments op 
join order_items oi 
on oi.order_id = op.order_id 
join products p 
on p.product_id = oi.product_id 
join product_category_name_translation pcnt 
on pcnt.product_category_name = p.product_category_name 
group by pcnt.product_category_name_english
order by total_sales desc 
limit 10

------------------------------
--Top 10 total order by state
------------------------------

select 
	g.geolocation_state as state,
	g.geolocation_zip_code_prefix, 
	count(o.order_id) as total_orders
from orders o 
join customers c 
on c.customer_id = o.customer_id 
join geolocation g 
on g.geolocation_zip_code_prefix = c.customer_zip_code_prefix 
group by g.geolocation_state, g.geolocation_zip_code_prefix 
order by total_orders desc
limit 10
-----------------------------
--Payment method distribution
-------------------------------

select 
	op.payment_type,
	count(op.payment_type) 
from order_payments op 
group by op.payment_type

---------------------------------
--Customer growth over time
---------------------------------

select
	date_trunc('month', o.order_purchase_timestamp::timestamp) as month,
	count(c.customer_id)
from customers c 
join orders o 
on c.customer_id = o.customer_id 
group by date_trunc('month', o.order_purchase_timestamp::timestamp)

------------------------------------------
select 
	date_trunc('hour', o.order_purchase_timestamp::timestamp) as hours,
	extract(dow from cast(o.order_purchase_timestamp as timestamp)) as day_of_week, 
	date_part('hour', cast(o.order_purchase_timestamp as timestamp)) as hour,
	sum(op.payment_value) as total_sales 
from order_payments op 
join orders o 
on o.order_id = op.order_id 
group by date_trunc('hour', o.order_purchase_timestamp::timestamp) ,extract(DOW from o.order_purchase_timestamp::timestamp) , 
	date_part('hour', cast(o.order_purchase_timestamp as timestamp)) 
order by day_of_week , hour
select 	
	sum(op.payment_value) as total_sales,
	count(o.order_id) as num_orders,
	count(distinct c.customer_id),
	(sum(oi.price + oi.freight_value)/count(oi.order_id)) as avg_order_value,
	count(p.product_category_name) as total_products
from orders o 
join order_payments op 
on o.order_id = op.order_id 
join customers c 
on c.customer_id = o.customer_id 
join order_items oi 
on oi.order_id = o.order_id 
join products p 
on p.product_id = oi.product_id 
----------------------------------------
select
	date_trunc('month', o.order_purchase_timestamp::timestamp) as months,
	op.payment_value as sales
from order_payments op 
join orders o 
on o.order_id = op.order_id 

-------------------------------
--Customer distribution by state
-------------------------------

SELECT 
    COUNT(DISTINCT c.customer_id) AS num_customers,
    'BR-' || g.geolocation_state AS state
FROM customers c 
JOIN geolocation g 
ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix 
GROUP BY g.geolocation_state

------------------------------
--Total spend per Segment
------------------------------

WITH rfm_table AS (
    -- CTE: calculate RFM
    SELECT 
        distinct c.customer_id,
        MAX(CAST(o.order_purchase_timestamp AS TIMESTAMP)) AS last_purchase_date,
        COUNT(DISTINCT o.order_id) AS num_orders,
        SUM(op.payment_value) AS total_spent
    FROM orders o 
    JOIN order_payments op ON o.order_id = op.order_id 
    JOIN customers c ON c.customer_id = o.customer_id 
    GROUP BY distinct c.customer_id 
),
rfm_calc AS (
    SELECT 
        distinct customer_id,
        rfm_recency,
        rfm_frequency,
        rfm_monetary_value,
        CAST(rfm_recency AS TEXT) || CAST(rfm_frequency AS TEXT) || CAST(rfm_monetary_value AS TEXT) AS rfm_cell
    FROM (
        SELECT 
            customer_id,
            NTILE(5) OVER (ORDER BY DATE_TRUNC('month', last_purchase_date)) AS rfm_recency,
            NTILE(5) OVER (ORDER BY num_orders) AS rfm_frequency,
            NTILE(5) OVER (ORDER BY total_spent) AS rfm_monetary_value
        FROM rfm_table
    ) AS rfm_calc_table
)
SELECT 
    rfm_segment,
    sum(op.payment_value) AS total_spend
FROM (
    SELECT 
        distinct customer_id,
        rfm_recency,
        rfm_frequency,
        rfm_monetary_value,
        rfm_cell,
        CASE 
            WHEN rfm_recency IN ('4','5') AND rfm_frequency IN ('4', '5') AND rfm_monetary_value IN ('4', '5') THEN 'Champions'
            WHEN rfm_recency BETWEEN '2' AND '5' AND rfm_frequency BETWEEN '3' AND '5' AND rfm_monetary_value BETWEEN '3' AND '5' THEN 'Loyal Customers'
            WHEN rfm_recency BETWEEN '3' AND '5' AND rfm_frequency BETWEEN '1' AND '3' AND rfm_monetary_value BETWEEN '1' AND '3' THEN 'Potential Loyalist'
            WHEN rfm_recency BETWEEN '4' AND '5' AND rfm_frequency BETWEEN '0' AND '1' AND rfm_monetary_value BETWEEN '0' AND '1' THEN 'Recent Customers'
            WHEN rfm_recency BETWEEN '3' AND '4' AND rfm_frequency BETWEEN '0' AND '1' AND rfm_monetary_value BETWEEN '0' AND '1' THEN 'Promising'
            WHEN rfm_recency BETWEEN '2' AND '3' AND rfm_frequency BETWEEN '2' AND '3' AND rfm_monetary_value BETWEEN '2' AND '3' THEN 'Customers Needing Attention'
            WHEN rfm_recency BETWEEN '2' AND '3' AND rfm_frequency BETWEEN '0' AND '2' AND rfm_monetary_value BETWEEN '0' AND '2' THEN 'About To Sleep'
            WHEN rfm_recency BETWEEN '0' AND '2' AND rfm_frequency BETWEEN '2' AND '5' AND rfm_monetary_value BETWEEN '2' AND '5' THEN 'At Risk'
            WHEN rfm_recency BETWEEN '0' AND '1' AND rfm_frequency BETWEEN '4' AND '5' AND rfm_monetary_value BETWEEN '4' AND '5' THEN 'Can’t Lose Them'
            WHEN rfm_recency BETWEEN '1' AND '2' AND rfm_frequency BETWEEN '1' AND '2' AND rfm_monetary_value BETWEEN '1' AND '2' THEN 'Hibernating'
            WHEN rfm_recency BETWEEN '0' AND '2' AND rfm_frequency BETWEEN '0' AND '2' AND rfm_monetary_value BETWEEN '0' AND '2' THEN 'Lost'
            ELSE 'Other' 
        END AS rfm_segment
    FROM rfm_calc
) AS rfm_segments
JOIN orders o   ON rfm_segments.customer_id = o.customer_id
join order_payments op on o.order_id = op.order_id 
GROUP BY rfm_segment
ORDER BY total_spend DESC;

------------------------------------------
SELECT 
    COUNT(CASE WHEN order_status = 'delivered' THEN 1 END) * 1.0 / COUNT(*) AS delivery_success_rate
FROM orders;

SELECT 
    DATE_TRUNC('month', o.order_purchase_timestamp::timestamp) AS month,
    COUNT(*) AS total_orders,
    COUNT(CASE WHEN o.order_status = 'delivered' THEN 1 END) AS delivered_orders,
    COUNT(CASE WHEN o.order_status = 'delivered' THEN 1 END) * 1.0 / COUNT(*) as Success_Rate
FROM orders o
GROUP BY DATE_TRUNC('month', o.order_purchase_timestamp::timestamp)
ORDER BY month;
------------------------------------------
WITH delivery_times AS (
    SELECT 
        o.order_id,
        c.customer_id,
        g.geolocation_state AS state,
        AGE(cast(o.order_delivered_customer_date as timestamp), cast(o.order_purchase_timestamp as timestamp)) AS delivery_time
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN geolocation g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
    WHERE o.order_status = 'delivered'
)
SELECT 
    'BR-' || state AS region,
    EXTRACT(EPOCH FROM delivery_time) / 3600 AS delivery_hours
FROM delivery_times
group by region, delivery_hours

------------------------------------------
WITH delivery_time AS (
    SELECT 
        o.order_id,
        o.customer_id,
        EXTRACT(EPOCH FROM (cast(o.order_delivered_customer_date as timestamp) - cast(o.order_purchase_timestamp as timestamp))) / 3600 AS delivery_hours,
        SUM(op.payment_value) AS order_value
    FROM orders o
    JOIN order_payments op ON o.order_id = op.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY o.order_id, o.customer_id, o.order_delivered_customer_date, o.order_purchase_timestamp
)
SELECT 
    customer_id,
    delivery_hours,
    order_value
FROM delivery_time;
------------------------------------
--Sales by day of the week and Hours
------------------------------------
WITH sales_data AS (
    SELECT 
        CASE 
            WHEN EXTRACT(DOW FROM cast(o.order_purchase_timestamp as timestamp)) = 0 THEN 'Sunday'
            WHEN EXTRACT(DOW FROM cast(o.order_purchase_timestamp as timestamp)) = 1 THEN 'Monday'
            WHEN EXTRACT(DOW FROM cast(o.order_purchase_timestamp as timestamp)) = 2 THEN 'Tuesday'
            WHEN EXTRACT(DOW FROM cast(o.order_purchase_timestamp as timestamp)) = 3 THEN 'Wednesday'
            WHEN EXTRACT(DOW FROM cast(o.order_purchase_timestamp as timestamp)) = 4 THEN 'Thursday'
            WHEN EXTRACT(DOW FROM cast(o.order_purchase_timestamp as timestamp)) = 5 THEN 'Friday'
            WHEN EXTRACT(DOW FROM cast(o.order_purchase_timestamp as timestamp)) = 6 THEN 'Saturday'
        END AS day_of_week,
        EXTRACT(HOUR FROM cast(o.order_purchase_timestamp as timestamp)) AS hour_of_day,
        SUM(op.payment_value) AS total_sales
    FROM orders o
    JOIN order_payments op ON o.order_id = op.order_id
    GROUP BY day_of_week, hour_of_day
)
SELECT 
    day_of_week,
    hour_of_day,
    total_sales
FROM sales_data
ORDER BY 
    CASE 
        WHEN day_of_week = 'Sunday' THEN 0
        WHEN day_of_week = 'Monday' THEN 1
        WHEN day_of_week = 'Tuesday' THEN 2
        WHEN day_of_week = 'Wednesday' THEN 3
        WHEN day_of_week = 'Thursday' THEN 4
        WHEN day_of_week = 'Friday' THEN 5
        WHEN day_of_week = 'Saturday' THEN 6
    END,
    hour_of_day;






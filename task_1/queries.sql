#1. Tracking Revenue Trends by Country and Currency

SELECT 
    d.country,
    f.currency,
    SUM(f.amount) AS total_revenue
FROM Fact_Orders f
JOIN Dim_Address d ON f.address_id_billing = d.address_id
GROUP BY d.country, f.currency;


#2. Analyzing Customer Activity and Order History

SELECT 
    c.name AS customer_name,
    COUNT(f.order_id) AS total_orders,
    SUM(f.amount) AS total_spent
FROM Fact_Orders f
JOIN Dim_Customer c ON f.customer_id = c.customer_id
GROUP BY c.name;

#3. Monitoring Shipping Performance and Identifying Delays

SELECT 
    d.country,
    f.shipping_status,
    COUNT(f.order_id) AS total_shipments
FROM Fact_Orders f
JOIN Dim_Address d ON f.address_id_shipping = d.address_id
GROUP BY d.country, f.shipping_status;
